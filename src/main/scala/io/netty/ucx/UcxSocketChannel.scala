
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.UcxCallback
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelPromise
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.ChannelException
import io.netty.channel.FileRegion
import io.netty.channel.socket.DuplexChannel
import io.netty.channel.socket.SocketChannel
import io.netty.channel.unix.UnixChannelUtil
import io.netty.util.concurrent.GlobalEventExecutor

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.channels.ClosedChannelException
import java.nio.channels.WritableByteChannel
import java.net.SocketAddress
import java.net.InetSocketAddress
import java.util.Collection
import java.util.Collections
import java.util.Map
import java.util.concurrent.Executor

/**
 * {@link SocketChannel} implementation that uses linux EPOLL Edge-Triggered Mode for
 * maximal performance.
 */
class UcxSocketChannel(parent: Channel)
    extends AbstractUcxChannel(parent) with SocketChannel with UcxLogging {

    protected val ucxSocketConfig = new UcxSocketChannelConfig(this)
    protected var underlyingUnsafe: UcxClientUnsafe = _

    protected var bOutputShutdown = false
    protected var bInputShutdown = false

    protected lazy val flushOut = new Runnable() {
        override
        def run(): Unit = underlyingUnsafe.flush0()
    }

    def this() = {
        this(null)
    }

    override
    def ucxUnsafe(): AbstractUcxUnsafe = underlyingUnsafe

    override
    def config() = ucxSocketConfig

    override
    def remoteAddress(): InetSocketAddress = super.remoteAddress().asInstanceOf[InetSocketAddress]

    override
    def localAddress(): InetSocketAddress = super.localAddress().asInstanceOf[InetSocketAddress]

    override
    def parent(): UcxServerSocketChannel = super.parent().asInstanceOf[UcxServerSocketChannel]

    override
    protected def doConnect(remoteAddress: SocketAddress, localAddress: SocketAddress): Unit = {
        remote = remoteAddress.asInstanceOf[java.net.InetSocketAddress]
        ucxUnsafe.setSocketAddress(remote)
        ucxUnsafe.doConnect0()
    }

    override
    protected def doWrite(in: ChannelOutboundBuffer): Unit = {
        var writeSpinCount = config().getWriteSpinCount()
        do {
            val msgCount = in.size()
            // Do gathering write if the outbound buffer entries start with more than one ByteBuf.
            if (msgCount == 0) {
                return
            } else if (msgCount > 1 && in.current().isInstanceOf[ByteBuf]) {
                writeSpinCount -= doWriteMultiple(in)
            } else {  // msgCount == 1
                writeSpinCount -= doWriteSingle(in)
            }

            // We do not break the loop here even if the outbound buffer was flushed completely,
            // because a user might have triggered another write and flush when we notify his or her
            // listeners.
        } while (writeSpinCount > 0)

        if (writeSpinCount == 0) {
            // We used our writeSpin quantum, and should try to write again later.
            eventLoop().execute(flushOut)
        } else {
            // Underlying descriptor can not accept all data currently
            // TODO: ???
        }
    }

    private def doWriteMultiple(in: ChannelOutboundBuffer): Int = {
        // TODO: Am doesn't support IOV yet.
        var num = 0
        while (in.size() > 0) {
            num += doWriteSingle(in)
        }
        return num
    }

    protected def doWriteSingle(in: ChannelOutboundBuffer): Int = {
        // The outbound directBuf contains only one message or it contains a file region.
        val msg = in.current()
        msg match {
        case buf: ByteBuf =>
            return writeBytes(in, buf)
        case fr: FileRegion =>
            return writeFileRegion(in, fr)
        // TODO: support ucx registered memory
        // case msg: UcxMemory => return writeUcpMemory(in, (FileRegion) msg)
        case _ =>
            // Should never reach here.
            throw new UnsupportedOperationException(
                s"unsupported message type: ${msg.getClass}")
        }
    }

    private def writeFileRegion(in: ChannelOutboundBuffer, region: FileRegion): Int = {
        val offset = region.transferred()
        val regionCount = region.count()
        if (offset >= regionCount) {
            in.remove()
            return 0
        }

        // TODO: read file limit
        val maxWrite = 16 * 1024 * 1024
        var byteChannel: UcxWritableByteChannel = null
        try {
            byteChannel = new UcxWritableByteChannel(config().getAllocator(), maxWrite)
            val flushedAmount = region.transferTo(byteChannel, offset)   

            if (flushedAmount > 0) {  
                val doneCb = () => byteChannel.close()
                val buf = byteChannel.internalByteBuf()       

                if (buf.hasMemoryAddress() || buf.nioBufferCount() == 1) {
                    return doWriteBytes(in, buf, doneCb)
                } else {
                    return doWriteBytesMultiple(in, buf, doneCb)
                }
            }
        } catch {
            case t: Throwable => {
                if (byteChannel != null) {
                    byteChannel.close()
                }
                throw t
            }
        }
        return AbstractUcxChannel.SNDBUF_FULL
    }

    private def writeBytes(in: ChannelOutboundBuffer, buf: ByteBuf): Int = {
        if (buf.readableBytes() == 0) {
            in.remove()
            return 0
        }

        if (buf.hasMemoryAddress() || buf.nioBufferCount() == 1) {
            return doWriteBytes(in, buf, in.remove _)
        } else {
            return doWriteBytesMultiple(in, buf, in.remove _)
        }
    }

    private def doWriteBytesMultiple(in: ChannelOutboundBuffer, buf: ByteBuf,
                                     doneCb: () => Unit): Int = {
        // TODO: support iov
        val nioBuffers = buf.nioBuffers()

        for (nioBuf <- nioBuffers) {
            val flushedAmount = nioBuf.limit() - nioBuf.position()
            val writeCb = () => doWriteBytesCb(in, buf, flushedAmount, doneCb)

            underlyingUnsafe.doWrite0(nioBuf, writeCb)
        }

        return 1
    }

    private def doWriteBytes(in: ChannelOutboundBuffer, buf: ByteBuf,
                             doneCb: () => Unit): Int = {
        if (buf.hasMemoryAddress()) {
            val flushedAmount = buf.writerIndex() - buf.readerIndex()
            val writeCb = () => doWriteBytesCb(in, buf, flushedAmount, doneCb)

            underlyingUnsafe.doWrite0(buf.memoryAddress(), buf.readerIndex(), buf.writerIndex(), writeCb)
        } else { // buf.nioBufferCount() == 1
            val nioBuf = buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes())

            val flushedAmount = nioBuf.limit() - nioBuf.position()
            val writeCb = () => doWriteBytesCb(in, buf, flushedAmount, doneCb)

            underlyingUnsafe.doWrite0(nioBuf, writeCb)
        }

        return 1
    }

    private def doWriteBytesCb(in: ChannelOutboundBuffer, buf: ByteBuf,
                               flushedAmount: Int, doneCb: () => Unit): Unit = {
        in.removeBytes(flushedAmount)
        if (buf.readableBytes == 0) {
            doneCb()
        }
    }

    override
    protected def filterOutboundMessage(msg: Object): Object = {
        msg match {
            case buf: ByteBuf => {
                val buf = msg.asInstanceOf[ByteBuf]
                if (UnixChannelUtil.isBufferCopyNeededForWrite(buf)) {
                    newDirectBuffer(buf)
                } else {
                    buf
                }
                    
            }
            case fr: FileRegion => fr
            case _ => 
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
        }
    }

    override
    def doReadAmData(ucpAmData: UcpAmData): Unit = {
        val readableBytes = ucpAmData.getLength.toInt
        val pipe = pipeline()

        var byteChannel: UcxWritableByteChannel = null
        try {
            byteChannel = new UcxWritableByteChannel(
                config().getAllocator(), readableBytes)

            val buf = byteChannel.internalByteBuf()
            val readCb = () => {
                val _ = pipe.fireChannelRead(buf)
            }

            if (ucpAmData.isDataValid()) {
                val ucpBuf = UnsafeUtils.getByteBufferView(
                    ucpAmData.getDataAddress, readableBytes)
                byteChannel.write(ucpBuf)
                readCb()
            } else {
                underlyingUnsafe.doRead0(ucpAmData, buf, readCb) 
            }
        } catch {
            case t: Throwable => {
                if (byteChannel != null) {
                    byteChannel.close()
                }
                throw t
            }
        }
    }

    override
    protected def newUnsafe(): AbstractUcxUnsafe = {
        underlyingUnsafe = new UcxClientUnsafe()
        underlyingUnsafe
    }

    override
    def isOutputShutdown() = bOutputShutdown

    def isInputShutdown() = bInputShutdown

    override
    def isShutdown() = bInputShutdown && bOutputShutdown

    override
    def shutdownOutput(): ChannelFuture = shutdownOutput(newPromise())

    override
    def shutdownOutput(promise: ChannelPromise): ChannelFuture = {
        eventLoopRun(() => underlyingUnsafe.shutdownOut0(promise))
        promise
    }

    override
    protected def doShutdownOutput(): Unit = {
        shutdownOutput()
    }

    override
    def shutdownInput(): ChannelFuture = shutdownInput(newPromise())

    override
    def shutdownInput(promise: ChannelPromise): ChannelFuture = {
        eventLoopRun(() => underlyingUnsafe.shutdownIn0(promise))
        promise
    }

    override
    def shutdown(): ChannelFuture = {
        return shutdown(newPromise())
    }

    override
    def shutdown(promise: ChannelPromise): ChannelFuture = {
        val shutdownOutputFuture = shutdownOutput()
        if (shutdownOutputFuture.isDone()) {
            shutdownOutputDone(shutdownOutputFuture, promise)
        } else {
            shutdownOutputFuture.addListener(new ChannelFutureListener() {
                override
                def operationComplete(shutdownOutputFuture: ChannelFuture): Unit = {
                    shutdownOutputDone(shutdownOutputFuture, promise)
                }
            })
        }
        return promise
    }

    private def shutdownOutputDone(shutdownOutputFuture: ChannelFuture,
                                   promise: ChannelPromise): Unit = {
        val shutdownInputFuture = shutdownInput()
        if (shutdownInputFuture.isDone()) {
            shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise)
        } else {
            shutdownInputFuture.addListener(new ChannelFutureListener() {
                override
                def operationComplete(shutdownInputFuture: ChannelFuture): Unit = {
                    shutdownDone(shutdownOutputFuture, shutdownInputFuture, promise)
                }
            })
        }
    }

    private def shutdownDone(shutdownOutputFuture: ChannelFuture,
                             shutdownInputFuture: ChannelFuture,
                             promise: ChannelPromise): Unit = {
        val shutdownOutputCause = shutdownOutputFuture.cause()
        val shutdownInputCause = shutdownInputFuture.cause()
        if (shutdownOutputCause != null) {
            if (shutdownInputCause != null) {
                logDebug("Exception suppressed because a previous exception occurred.",
                          shutdownInputCause)
            }
            promise.setFailure(shutdownOutputCause)
        } else if (shutdownInputCause != null) {
            promise.setFailure(shutdownInputCause)
        } else {
            promise.setSuccess()
        }
    }

    override
    protected def doClose(): Unit = {
        try {
            super.doClose()
        } finally {
            // TODO
            ucxUnsafe.doClose0()
        }
    }

    /**
      * client: epOut -> CONNECT(epOut id) -> CONNECT_REPLY(epIn id) -> epIn
      * server: CONNECT(epOut id) -> epIn -> epOut -> CONNECT_REPLY(epIn id)
      */
    class UcxClientUnsafe extends AbstractUcxUnsafe {
        val ucpWorker = ucxEventLoop.ucpWorker
        var ucpEpOut: UcpEndpoint = _
        var ucpEpIn: UcpEndpoint = _
        var ucpAddress: ByteBuffer = _
        var ucpSocketAddress: InetSocketAddress = _

        private[ucx] def doWrite0(buf: ByteBuffer, writeCb: () => Unit): Int = {
            doWrite0(UnsafeUtils.getAddress(buf), buf.position(), buf.limit(), writeCb)
        }

        private[ucx] def doWrite0(address: Long, offset: Int, limit: Int, writeCb: () => Unit): Int = {
            val header = remoteId.directBuffer()

            ucpEpOut.sendAmNonBlocking(
                UcxAmId.MESSAGE,
                UnsafeUtils.getAddress(header), header.remaining(),
                address + offset, limit - offset, 0,
                new UcxCallback() {
                    override def onSuccess(request: UcpRequest): Unit = {
                        writeCb()
                        header.clear()
                    }
                    override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                        logError(s"Failed to send $errorMsg")
                    }
                },
                MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            return 1
        }

        private[ucx] def doRead0(ucpAmData: UcpAmData, buf: ByteBuf,
                                 readCb: () => Unit):Unit = {
            var address: Long = 0

            if (buf.hasMemoryAddress()) {
                address = buf.memoryAddress()
            } else if (buf.nioBufferCount() == 1) {
                address = UnsafeUtils.getAddress(buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes()))
            } else {
                throw new UnsupportedOperationException(s"buf count ${buf.nioBufferCount()} > 1")
            }

            ucpWorker.recvAmDataNonBlocking(
                ucpAmData.getDataHandle, address, ucpAmData.getLength,
                new UcxCallback() {
                    override def onSuccess(r: UcpRequest): Unit = {
                        readCb()
                    }
                }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        }

        override
        def flush0(): Unit = {
            ucpEpOut.flushNonBlocking(null)
        }

        override
        def doConnect0():Unit = {
            try {
                ucpEpOut = ucpWorker.newEndpoint(ucpEpParam)
                local = ucpEpOut.getLocalAddress()
                opened = true

                // Tell remote which id this side uses.
                val header = uniqueId.directBuffer()
                val workerAddress = ucpWorker.getAddress()

                ucpEpOut.sendAmNonBlocking(
                    UcxAmId.CONNECT, UnsafeUtils.getAddress(header), header.remaining(),
                    UnsafeUtils.getAddress(workerAddress), workerAddress.remaining(),
                    UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
                    new UcxCallback() {
                        override def onSuccess(request: UcpRequest): Unit = {
                            header.clear()
                            workerAddress.clear()
                            logDebug(s"connecting $local -> $remote")
                        }
                        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                            logError(s"connecting $local -x-> $remote: $errorMsg")
                        }
                    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            } catch {
                case _: Throwable => {
                    doClose0()
                }
            }
        }

        override
        def doConnectDone0(): Unit = {
            connectPromise.setSuccess()

            bInputShutdown = false
            bOutputShutdown = false

            active = true

            logDebug(s"connected $local <-> $remote")
        }

        override
        def doAccept0(epIn: UcpEndpoint):Unit = {
            ucpEpIn = epIn

            local = ucpEpIn.getLocalAddress()
            remote = ucpEpIn.getRemoteAddress()

            // client doesn't know unique id yet.
            ucxEventLoop.addChannel(ucpEpIn.getNativeId(), UcxSocketChannel.this)

            logDebug(s"connected $local <-> $remote")
        }

        override
        def doConnectBack0():Unit = {
            try {
                ucpEpOut = ucpWorker.newEndpoint(ucpEpParam)
                opened = true

                // Tell remote which id this side uses.
                val headerSize = UnsafeUtils.LONG_SIZE + UnsafeUtils.LONG_SIZE
                val header = ByteBuffer.allocateDirect(headerSize)
                val headerAddress = UnsafeUtils.getAddress(header)

                header.putLong(remoteId.get())
                header.putLong(uniqueId.get())

                ucpEpOut.sendAmNonBlocking(
                    UcxAmId.REPLY_CONNECT, headerAddress, headerSize, headerAddress, 0,
                    UcpConstants.UCP_AM_SEND_FLAG_EAGER,
                    new UcxCallback() {
                        override def onSuccess(request: UcpRequest): Unit = {
                            header.clear()
                            doConnectedBackDone0()
                            logDebug(s"replying $local -> $remote")
                        }
                        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                            logError(s"replying $local -> $remote: $errorMsg")
                        }
                    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

            } catch {
                case _: Throwable => {
                    doClose0()
                }
            }
        }

        override
        def doConnectedBackDone0(): Unit = {
            // client received unique id now.
            ucxEventLoop.delChannel(ucpEpIn.getNativeId())

            bInputShutdown = false
            bOutputShutdown = false

            active = true

            logDebug(s"connected $local <-> $remote")
        }

        override
        def doClose0():Unit = {
            eventLoopRun(() => {
                if (ucpEpIn != null) {
                    shutdownIn0(newPromise())
                }

                if (ucpEpOut != null) {
                    shutdownOut0(newPromise())
                }
            })
        }

        private[ucx] def shutdownIn0(promise: ChannelPromise):Unit = {
            if (ucpEpIn != null) {
                shutdown0(ucpEpIn, promise)
                bInputShutdown = true
                ucpEpIn = null
            }
        }

        private[ucx] def shutdownOut0(promise: ChannelPromise):Unit = {
            if (ucpEpOut != null) {
                shutdown0(ucpEpOut, promise)
                bInputShutdown = true
                ucpEpOut = null
            }
        }

        private[ucx] def shutdown0(ucpEp: UcpEndpoint, promise: ChannelPromise):Unit = {
            val closing = ucpEp.closeNonBlockingFlush()

            while (!closing.isCompleted) {
                ucpWorker.progress()
            }

            promise.setSuccess()
        }

        override
        def setSocketAddress(address: InetSocketAddress): Unit = {
            ucpEpParam.setSocketAddress(address)
                .sendClientId()
                .setPeerErrorHandlingMode()
                .setErrorHandler(ucpErrHandler(s"Endpoint to $address got an error"))
                .setName(s"Endpoint to ${address}")
            ucpSocketAddress = address
        }

        override
        def setUcpAddress(address: ByteBuffer): Unit = {
            val name = StandardCharsets.UTF_8.decode(address.slice()).toString
            ucpEpParam.setUcpAddress(address)
                .sendClientId()
                .setName(s"Endpoint to ${name}")
            ucpAddress = address
        }
    }
}

private[ucx] class UcxWritableByteChannel(
    alloc: ByteBufAllocator, maxWrite: Int)
    extends WritableByteChannel {

    protected var bOpen = true
    protected var directBuf: ByteBuf = null

    protected def allocBuf(size: Int): ByteBuf = {
        if (alloc.isDirectBufferPooled()) {
            return alloc.directBuffer(size)
        }

        val directBuf = ByteBufUtil.threadLocalDirectBuffer()
        if (directBuf != null) {
            return directBuf
        }

        return Unpooled.directBuffer(size)
    }

    def internalByteBuf(): ByteBuf = directBuf

    def write(src: ByteBuffer): Int = {
        val position = src.position()
        val limit = src.limit()
        val readableBytes = (limit - position).min(maxWrite)

        if (readableBytes == 0) {
            return 0
        }

        if (directBuf == null) {
            directBuf = allocBuf(readableBytes)
        }

        val before = directBuf.readerIndex()
        directBuf.writeBytes(src)
        val after = directBuf.readerIndex()
        val written = after - before

        src.position(position + written)
        return written
    }

    def close(): Unit = {
        bOpen = false
        if (directBuf != null) {
            directBuf.release()
        }
    }

    def isOpen() = bOpen
}