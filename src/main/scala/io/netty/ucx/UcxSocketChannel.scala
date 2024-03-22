
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
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
import io.netty.channel.socket.SocketChannel
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
class UcxSocketChannel(parent: UcxServerSocketChannel)
    extends AbstractUcxChannel(parent) with SocketChannel with UcxLogging {
    logDev(s"UcxSocketChannel()")

    protected val ucxSocketConfig = new UcxSocketChannelConfig(this)
    protected var underlyingUnsafe: UcxClientUnsafe = _

    @volatile protected var bOutputShutdown = false
    @volatile protected var bInputShutdown = false

    protected lazy val flushOut = new Runnable() {
        override
        def run(): Unit = underlyingUnsafe.flush0()
    }

    def this() = {
        this(null)
    }

    override
    def ucxUnsafe: AbstractUcxUnsafe = underlyingUnsafe

    override
    def config(): UcxSocketChannelConfig = ucxSocketConfig

    override
    def remoteAddress(): InetSocketAddress = super.remoteAddress().asInstanceOf[InetSocketAddress]

    override
    def localAddress(): InetSocketAddress = super.localAddress().asInstanceOf[InetSocketAddress]

    override
    def parent(): UcxServerSocketChannel = super.parent().asInstanceOf[UcxServerSocketChannel]

    override
    protected def doConnect(remoteAddress: SocketAddress, localAddress: SocketAddress): Unit = {
        logDev(s"doConnect() $localAddress -> $remoteAddress")

        ucxUnsafe.setSocketAddress(remoteAddress.asInstanceOf[java.net.InetSocketAddress])

        eventLoopRun(() => ucxUnsafe.doConnect0())
    }

    override
    protected def doWrite(in: ChannelOutboundBuffer): Unit = {
        logDev(s"doWrite() $in")
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
        val toWrite = (regionCount - offset).toInt.min(maxWrite)
        var byteChannel: UcxWritableByteChannel = null
        try {
            byteChannel = new UcxWritableByteChannel(config().getAllocator(), toWrite)
            val flushedAmount = region.transferTo(byteChannel, offset)   

            if (flushedAmount > 0) {  
                val doneCb = byteChannel.close _
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
            return doWriteBytes(in, buf, () => {})
        } else {
            return doWriteBytesMultiple(in, buf, () => {})
        }
    }

    private def doWriteBytesMultiple(in: ChannelOutboundBuffer, buf: ByteBuf,
                                     doneCb: () => Unit): Int = {
        // TODO: support iov
        val nioBuffers = buf.nioBuffers()

        for (nioBuf <- nioBuffers) {
            val flushedAmount = nioBuf.limit() - nioBuf.position()
            val writeCb = doWriteBytesCb(in, buf, flushedAmount, doneCb)

            eventLoopRun(() => underlyingUnsafe.doWrite0(nioBuf, writeCb))
        }

        return 1
    }

    private def doWriteBytes(in: ChannelOutboundBuffer, buf: ByteBuf,
                             doneCb: () => Unit): Int = {
        if (buf.hasMemoryAddress()) {
            val flushedAmount = buf.writerIndex() - buf.readerIndex()
            val writeCb = doWriteBytesCb(in, buf, flushedAmount, doneCb)

            eventLoopRun(() => underlyingUnsafe.doWrite0(
                buf.memoryAddress(), buf.readerIndex(), buf.writerIndex(), writeCb))
        } else { // buf.nioBufferCount() == 1
            val nioBuf = buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes())

            val flushedAmount = nioBuf.limit() - nioBuf.position()
            val writeCb = doWriteBytesCb(in, buf, flushedAmount, doneCb)

            eventLoopRun(() => underlyingUnsafe.doWrite0(nioBuf, writeCb))
        }

        return 1
    }

    private def doWriteBytesCb(in: ChannelOutboundBuffer, buf: ByteBuf,
                               flushedAmount: Int, doneCb: () => Unit): UcxCallback = {
        new UcxCallback() {
            override def onSuccess(request: UcpRequest): Unit = {
                in.removeBytes(flushedAmount)
                if (buf.readableBytes() == 0) {
                    in.remove()
                }
                doneCb()
                logTrace(s"$local MESSAGE $remote: success")
            }
            override def onError(status: Int, errorMsg: String): Unit = {
                doneCb()
                throw new UcxException(s"$local MESSAGE $remote: $errorMsg", status)
            }
        }
    }

    protected[ucx] def isBufferCopyNeededForWrite(byteBuf: ByteBuf): Boolean = {
        return !byteBuf.hasMemoryAddress() && !byteBuf.isDirect();
    }

    override
    protected def filterOutboundMessage(msg: Object): Object = {
        msg match {
            case buf: ByteBuf => {
                val buf = msg.asInstanceOf[ByteBuf]
                if (isBufferCopyNeededForWrite(buf)) {
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

        var directBuf: ByteBuf = null
        try {
            directBuf = PreferredDirectByteBufAllocator.directBuffer0(
                config().getAllocator(), readableBytes)

            val readCb = new UcxCallback() {
                override def onSuccess(r: UcpRequest): Unit = {
                    directBuf.writerIndex(readableBytes)
                    pipe.fireChannelRead(directBuf).fireChannelReadComplete()
                    logTrace(s"Read MESSAGE from $remote success: $directBuf")
                }
                override def onError(status: Int, errorMsg: String): Unit = {
                    val e = new UcxException(s"Read MESSAGE from $remote fail: $errorMsg", status)
                    pipe.fireChannelReadComplete().fireExceptionCaught(e)
                }
            }

            if (ucpAmData.isDataValid()) {
                val ucpBuf = UnsafeUtils.getByteBufferView(
                    ucpAmData.getDataAddress, readableBytes)
                directBuf.writeBytes(ucpBuf)
                readCb.onSuccess(null)
            } else {
                underlyingUnsafe.doRead0(ucpAmData, directBuf, readCb) 
            }
        } catch {
            case t: Throwable => {
                if (directBuf != null) {
                    directBuf.release()
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
        eventLoopRun(() => try {
            ucxUnsafe.doClose0()
        } catch {
            case e: Throwable => pipeline().fireExceptionCaught(e)
        })

        super.doClose()
    }

    /**
      * client: accept -> CONNECT(epOut id) -- CONNECT_ACK(epIn id) -> connected
      * server: CONNECT(epOut id) -> CONNECT_ACK(epIn id)
      */
    class UcxClientUnsafe extends AbstractUcxUnsafe {
        val ucpErrHandler = new UcpEndpointErrorHandler() {
            override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
                logWarning(s"Connection to $remote: $errorMsg")
                opened = false
                close(voidPromise())
            }
        }

        val ucpEpParam = new UcpEndpointParams()
        var ucpEp: UcpEndpoint = _

        var actionEpAddress: ByteBuffer = _
        var actionEpParam: UcpEndpointParams = _
        var actionEp: UcpEndpoint = _

        private[ucx] def doWrite0(buf: ByteBuffer, writeCb: UcxCallback): Int = {
            doWrite0(UnsafeUtils.getAddress(buf), buf.position(), buf.limit(), writeCb)
        }

        private[ucx] def doWrite0(address: Long, offset: Int, limit: Int, writeCb: UcxCallback): Int = {
            val header = remoteId.directBuffer()

            logDev(s"$local MESSAGE $remote: ongoing")
            actionEp.sendAmNonBlocking(
                UcxAmId.MESSAGE,
                UnsafeUtils.getAddress(header), header.remaining(),
                address + offset, limit - offset, 0, writeCb,
                MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            return 1
        }

        private[ucx] def doRead0(ucpAmData: UcpAmData, buf: ByteBuf,
                                 readCb: UcxCallback): Unit = {
            var address: Long = 0

            if (buf.hasMemoryAddress()) {
                address = buf.memoryAddress()
            } else if (buf.nioBufferCount() == 1) {
                address = UnsafeUtils.getAddress(buf.internalNioBuffer(buf.writerIndex(), buf.writableBytes()))
            } else {
                throw new UnsupportedOperationException(s"buf count ${buf.nioBufferCount()} > 1")
            }

            ucpWorker.recvAmDataNonBlocking(
                ucpAmData.getDataHandle, address, ucpAmData.getLength, readCb,
                UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        }

        override
        def doConnect0(): Unit = {
            try {
                ucpEp = ucpWorker.newEndpoint(ucpEpParam)

                logDebug(s"doConnect0 $local -> $remote")

                // Tell remote which id this side uses.
                val header = uniqueId.directBuffer()
                val workerAddress = ucpWorker.getAddress()

                logDev(s"$local CONNECT $remote: ongoing")
                ucpEp.sendAmNonBlocking(
                    UcxAmId.CONNECT, UnsafeUtils.getAddress(header), header.remaining(),
                    UnsafeUtils.getAddress(workerAddress), workerAddress.remaining(),
                    UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
                    new UcxCallback() {
                        override def onSuccess(request: UcpRequest): Unit = {
                            logDebug(s"$local CONNECT $remote: success")
                        }
                        override def onError(status: Int, errorMsg: String): Unit = {
                            // TODO raise error
                            connectFailed(status, s"$local CONNECT $remote: $errorMsg")
                            workerAddress.clear()
                            header.clear()
                        }
                    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

                local = ucpEp.getLocalAddress()
            } catch {
                case e: Throwable => {
                    logError(s"CONNECT $local -x-> $remote: $e $ucpEpParam")
                    doClose0()
                }
            }
        }

        override
        def doConnectedBack0(): Unit = {
            try {
                actionEp = ucpWorker.newEndpoint(actionEpParam)
                // Tell remote which id this side uses.
                val headerSize = UnsafeUtils.LONG_SIZE + UnsafeUtils.LONG_SIZE
                val header = ByteBuffer.allocateDirect(headerSize)
                val headerAddress = UnsafeUtils.getAddress(header)
                val workerAddress = ucpWorker.getAddress()

                header.putLong(remoteId.get())
                header.putLong(uniqueId.get())

                logDev(s"$local CONNECT_ACK $remote: ongoing")
                actionEp.sendAmNonBlocking(
                    UcxAmId.CONNECT_ACK, UnsafeUtils.getAddress(header), headerSize,
                    UnsafeUtils.getAddress(workerAddress), workerAddress.remaining(),
                    UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
                    new UcxCallback() {
                        override def onSuccess(request: UcpRequest): Unit = {
                            connectSuccess()
                            logDebug(s"$local CONNECT_ACK $remote: success")
                        }
                        override def onError(status: Int, errorMsg: String): Unit = {
                            connectFailed(status, s"$local CONNECT_ACK $remote: $errorMsg")
                            workerAddress.clear()
                            header.clear()
                        }
                    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            } catch {
                case e: Throwable => {
                    logError(s"CONNECT_ACK $local -> $remote: $e")
                    doClose0()
                }
            }
        }

        override
        def doConnectDone0(): Unit = {
            try {
                actionEp = ucpWorker.newEndpoint(actionEpParam)
                connectSuccess()
            } catch {
                case e: Throwable => {
                    connectFailed(UcsConstants.STATUS.UCS_ERR_IO_ERROR,
                                  s"$local HANDLE_ACK $remote: $e")
                }
            }
        }

        private def connectFailed(status: Int, errorMsg: String): Unit = {
            opened = false
            val e = new UcxException(errorMsg, status)
            if (connectPromise != null && connectPromise.tryFailure(e)) {
                close(voidPromise())
            }
        }

        private def connectSuccess(): Unit = {
            finishConnect(remote)

            bInputShutdown = false
            bOutputShutdown = false
 
            logDebug(s"connected $local <-> $remote")
        }

        override
        def doClose0(): Unit = {
            shutdown().sync()
        }

        private[ucx] def shutdownIn0(promise: ChannelPromise): Unit = {
            bInputShutdown = true
            promise.setSuccess()
        }

        private[ucx] def shutdownOut0(promise: ChannelPromise): Unit = {
            bOutputShutdown = true

            if (actionEp != null) {
                val closing = actionEp.closeNonBlockingFlush()
                while (!closing.isCompleted) {
                    ucpWorker.progress()
                }
                actionEp = null
            }

            promise.setSuccess()
        }

        override
        def setSocketAddress(address: InetSocketAddress): Unit = {
            ucpEpParam.setSocketAddress(address)
                .setPeerErrorHandlingMode()
                .setErrorHandler(ucpErrHandler)
                .setName(s"Ep to ${remote}")
            remote = address
        }

        override
        def setUcpAddress(address: ByteBuffer): Unit = {
            actionEpParam = new UcpEndpointParams().setUcpAddress(address)
                .setErrorHandler(ucpErrHandler)
                .setName(s"ActionEp to ${remote}")
            actionEpAddress = address
        }

        override
        def setActionEp(endpoint: UcpEndpoint): Unit = {
            actionEp = endpoint
        }

        override
        def setUcpEp(endpoint: UcpEndpoint): Unit = {
            connectPromise = newPromise()
            remote = endpoint.getRemoteAddress()
            local = endpoint.getLocalAddress()
            ucpEp = endpoint
        }
    }
}

private[ucx] class UcxWritableByteChannel(
    alloc: ByteBufAllocator, size: Int)
    extends WritableByteChannel with UcxLogging {

    protected var bOpen = true
    protected var directBuf: ByteBuf =
        PreferredDirectByteBufAllocator.directBuffer0(alloc, size)

    def internalByteBuf(): ByteBuf = directBuf

    def write(src: ByteBuffer): Int = {
        val dup = src.duplicate()
        val readableBytes = dup.remaining()
        if (readableBytes > size) {
            dup.limit(dup.position() + size)
        }

        directBuf.writeBytes(dup)

        val written = readableBytes - dup.remaining()
        src.position(dup.position())

        logDev(s"write() $dup $src $written")
        return written
    }

    def close(): Unit = {
        if (bOpen) {
            directBuf.release()
            bOpen = false
        }
    }

    def isOpen() = bOpen
}