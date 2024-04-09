
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelPromise
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion
import io.netty.channel.socket.SocketChannel
import io.netty.util.AbstractReferenceCounted

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel
import java.net.SocketAddress
import java.net.InetSocketAddress

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

    protected val flushTask = new Runnable() {
        override
        def run(): Unit = {
            underlyingUnsafe.ucpWorker.progress()
            underlyingUnsafe.flush0()
        }
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
        assert(eventLoop().inEventLoop())

        ucxUnsafe.setSocketAddress(remoteAddress.asInstanceOf[java.net.InetSocketAddress])
        ucxUnsafe.doConnect0()
    }

    override
    protected def doWrite(in: ChannelOutboundBuffer): Unit = {
        // write unfinished
        val spinLimit = config().getWriteSpinCount().min(in.size())
        for (i <- 0 until spinLimit) {
            val msg = in.current()
            msg match {
            case buf: ByteBuf =>
                writeByteBuf(buf)
            case fm: UcxFileRegionMsg =>
                writeFileMessage(fm)
            case _ =>
                // Should never reach here.
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
            }
            in.remove()
        }

        if (in.size() != 0) {
            eventLoop().execute(flushTask)
        }
    }

    protected def writeFileMessage(fm: UcxFileRegionMsg): Int = {
        var buf: ByteBuf = null
        try {
            buf = fm.newByteBuf()
            if (buf == null) {
                return 0
            }

            doWriteByteBuf(buf)
            return 1
        } catch {
            case t: Throwable => {
                if (buf != null) {
                    buf.release()
                }
                throw t
            }
        }
    }

    protected def writeByteBuf(buf: ByteBuf): Int = {
        if (buf.readableBytes() == 0) {
            return 0
        }
        // Increment refCounts here to let UcxCallback release
        buf.retain()

        doWriteByteBuf(buf)
        return 1
    }

    protected def doWriteByteBuf(buf: ByteBuf): Int = {
        val nioBufferCount = buf.nioBufferCount()
        val writeCb = newWriteUcxCallback(buf, nioBufferCount)

        val hasAddress = buf.hasMemoryAddress()
        val writeOnce = hasAddress || (nioBufferCount == 1)
        val readerIndex = buf.readerIndex()
        val readableBytes = buf.readableBytes()
        if (writeOnce) {
            if (hasAddress) {
                underlyingUnsafe.doWrite0(buf.memoryAddress(), readerIndex,
                                          buf.writerIndex(), writeCb)
            } else {
                val nioBuf = buf.internalNioBuffer(readerIndex, readableBytes)
                underlyingUnsafe.doWrite0(nioBuf, writeCb)
            }
        } else {
            buf.nioBuffers(readerIndex, readableBytes).foreach(
                underlyingUnsafe.doWrite0(_, writeCb))
        }

        return 1
    }

    protected def isBufferCopyNeededForWrite(byteBuf: ByteBuf): Boolean = {
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
            case fr: DefaultFileRegion => {
                new UcxDefaultFileRegionMsg(fr, config().getAllocator())
            }
            case fr: FileRegion => {
                new UcxFileRegionMsg(fr, config().getAllocator())
            }
            case _ => 
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
        }
    }

    protected def newWriteUcxCallback(buf: ByteBuf, refCounts: Int): UcxCallback = {
        new UcxSharedCallback(buf, refCounts, local, remote)
    }

    override
    def doReadAmData(ucpAmData: UcpAmData): Unit = {
        val readableBytes = ucpAmData.getLength.toInt
        val pipe = pipeline()

        var directBuf: ByteBuf = null
        try {
            directBuf = UcxPooledByteBufAllocator.directBuffer(
                config().getAllocator(), readableBytes, readableBytes)

            val readCb = new UcxCallback() {
                override def onSuccess(r: UcpRequest): Unit = {
                    directBuf.writerIndex(readableBytes)
                    pipe.fireChannelRead(directBuf).fireChannelReadComplete()
                    logDev(s"Read MESSAGE from $remote success: $directBuf")
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
                if (status == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                    logInfo(s"$ep: $errorMsg")
                } else {
                    logWarning(s"$ep: $errorMsg")
                }
                opened = false
                close(voidPromise())
                ucxEventLoop.delChannel(ep.getNativeId())
            }
        }

        val ucpEpParam = new UcpEndpointParams()
        var ucpEpAddress: ByteBuffer = _
        var ucpEp: UcpEndpoint = _

        private[ucx] def doWrite0(buf: ByteBuffer, writeCb: UcxCallback): Int = {
            doWrite0(UnsafeUtils.getAddress(buf), buf.position(), buf.limit(), writeCb)
        }

        private[ucx] def doWrite0(address: Long, offset: Int, limit: Int, writeCb: UcxCallback): Int = {
            val header = remoteId.directBuffer()

            logDev(s"$local MESSAGE $remote: ongoing($address $offset $limit)")
            ucpEp.sendAmNonBlocking(
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
        def doAccept0(): Unit = {
            try {
                logTrace(s"doAccept0 $local <- $remote")

                ucpEp = ucpWorker.newEndpoint(ucpEpParam)

                doExchangeId0()

                local = ucpEp.getLocalAddress()
            } catch {
                case e: Throwable => {
                    logError(s"ACCEPT $local <-x- $remote: $e $ucpEpParam")
                    doClose0()
                }
            }
        }

        override
        def doConnect0(): Unit = {
            try {
                logTrace(s"doConnect0 $local -> $remote")

                ucpEp = ucpWorker.newEndpoint(ucpEpParam)

                doExchangeId0()

                local = ucpEp.getLocalAddress()
            } catch {
                case e: Throwable => {
                    logError(s"CONNECT $local -x-> $remote: $e $ucpEpParam")
                    doClose0()
                }
            }
        }

        def doExchangeId0(): Unit = {
            // Tell remote which id this side uses.
            val nativeId = ucpEp.getNativeId()
            val header = uniqueId.directBuffer()

            uniqueId.set(nativeId)

            logDev(s"$local CONNECT $remote: ongoing")
            ucpEp.sendAmNonBlocking(
                UcxAmId.CONNECT, UnsafeUtils.getAddress(header), header.remaining(),
                UnsafeUtils.getAddress(header), 0,
                UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
                new UcxCallback() {
                    override def onSuccess(request: UcpRequest): Unit = {
                        logTrace(s"$local CONNECT $remote: success")
                    }
                    override def onError(status: Int, errorMsg: String): Unit = {
                        // TODO raise error
                        connectFailed(status, s"$local CONNECT $remote: $errorMsg")
                        header.clear()
                    }
                }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

            ucxEventLoop.addChannel(nativeId, UcxSocketChannel.this)
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

            if (ucpEp != null) {
                ucxEventLoop.delChannel(ucpEp.getNativeId())
                val closing = if (ucpEpAddress == null) {
                    ucpEp.closeNonBlockingForce()
                } else {
                    ucpEp.closeNonBlockingFlush()
                }
                while (!closing.isCompleted) {
                    ucpWorker.progress()
                }
                ucpEp = null
            }

            promise.setSuccess()
        }

        override
        def setConnectionRequest(request: UcpConnectionRequest): Unit = {
            val address = request.getClientAddress()
            ucpEpParam.setConnectionRequest(request)
                .setPeerErrorHandlingMode()
                .setErrorHandler(ucpErrHandler)
                .setName(s"Ep from ${address}")
            connectPromise = newPromise()
            remote = address
        }

        override
        def setSocketAddress(address: InetSocketAddress): Unit = {
            ucpEpParam.setSocketAddress(address)
                .setPeerErrorHandlingMode()
                .setErrorHandler(ucpErrHandler)
                .setName(s"Ep to ${address}")
            remote = address
        }

        override
        def setUcpAddress(address: ByteBuffer): Unit = {
            ucpEpParam.setUcpAddress(address)
                .setErrorHandler(ucpErrHandler)
                .setName(s"ActionEp to ${address}")
            ucpEpAddress = address
        }

        override
        def connectSuccess(): Unit = {
            underlyingUnsafe.finishConnect(remote)

            bInputShutdown = false
            bOutputShutdown = false

            logDebug(s"connected $local <-> $remote")
        }

        override
        def connectFailed(status: Int, errorMsg: String): Unit = {
            opened = false
            val e = new UcxException(errorMsg, status)
            if (connectPromise != null && connectPromise.tryFailure(e)) {
                close(voidPromise())
            }
        }

        override
        def connectReset(status: Int, errorMsg: String): Unit = {
            ucpErrHandler.onError(ucpEp, status, errorMsg)
        }
    }

    override
    def doRegister(): Unit = {
        if (parent != null) {
            underlyingUnsafe.doAccept0()
        }
    }
}

class UcxSharedCallback(buf: ByteBuf, private var refCounts: Int,
                        local: SocketAddress, remote: SocketAddress)
    extends UcxCallback with UcxLogging {
    override def onSuccess(request: UcpRequest): Unit = {
        release()
        logDev(s"$local MESSAGE $remote: success")
    }

    override def onError(status: Int, errorMsg: String): Unit = {
        release()
        throw new UcxException(s"$local MESSAGE $remote: $errorMsg", status)
    }

    @inline
    protected def release(): Unit = {
        refCounts -= 1
        if (refCounts == 0) {
            buf.release()
        }
    }
}

class UcxFileRegionMsg(fr: FileRegion, allocator: ByteBufAllocator)
    extends AbstractReferenceCounted {
    def newByteBuf(): ByteBuf = {
        // TODO: val maxWrite = 16 << 20 // .min(maxWrite)
        val offset = fr.transferred()
        val readableBytes = (fr.count() - offset).toInt
        if (readableBytes <= 0) {
            return null
        }

        val byteChannel = new UcxWritableByteChannel(allocator, readableBytes)

        fr.transferTo(byteChannel, offset)

        return byteChannel.internalByteBuf()
    }

    override
    def touch(): this.type = {
        return this
    }

    override
    def touch(hint: Object): this.type = {
        return this
    }

    override
    def deallocate(): Unit = {
        fr.release()
    }
}

class UcxDefaultFileRegionMsg(fr: DefaultFileRegion, allocator: ByteBufAllocator,
                              offset: Long, length: Long)
    extends UcxFileRegionMsg(fr, allocator) {

    def this(fr: DefaultFileRegion, allocator: ByteBufAllocator) = {
        this(fr, allocator, fr.position() + fr.transferred(),
             fr.count() - fr.transferred())
    }

    protected val fileChannel = UcxDefaultFileRegionMsg.getChannel(fr)

    override def newByteBuf(): ByteBuf = {
        val readableBytes = length.toInt
        val directBuf = UcxPooledByteBufAllocator.directBuffer(
            allocator, readableBytes, readableBytes)

        UcxDefaultFileRegionMsg.copyDefaultFileRegion(fileChannel, offset, length,
                                                      directBuf)
        return directBuf
    }
}

object UcxDefaultFileRegionMsg {
    private val clazz = classOf[DefaultFileRegion]
    private val fileField = clazz.getDeclaredField("file")

    fileField.setAccessible(true)

    def getChannel(fr: DefaultFileRegion): FileChannel = {
        fr.open()
        return fileField.get(fr).asInstanceOf[FileChannel]
    }

    def copyDefaultFileRegion(fileChannel: FileChannel, offset: Long,
                              length: Long, directBuf: ByteBuf): Unit = {
        // TODO: val maxWrite = 16 << 20 // .min(maxWrite)

        fileChannel.position(offset)

        val writerIndex = directBuf.writerIndex()
        if (directBuf.nioBufferCount() == 1) {
            val nioBuf = directBuf.internalNioBuffer(writerIndex, length.toInt)
            fileChannel.read(nioBuf)
        } else {
            fileChannel.read(directBuf.nioBuffers(writerIndex, length.toInt))
        }

        directBuf.writerIndex(writerIndex + length.toInt)
    }
}

class UcxWritableByteChannel(val alloc: ByteBufAllocator, val size: Int)
    extends WritableByteChannel with UcxLogging {

    protected var opened = true
    protected var directBuf: ByteBuf =
        UcxPooledByteBufAllocator.directBuffer(alloc, size, size)

    def internalByteBuf(): ByteBuf = directBuf

    override def write(src: ByteBuffer): Int = {
        val dup = src.duplicate()
        val readableBytes = dup.remaining()
        if (readableBytes > size) {
            dup.limit(dup.position() + size)
        }

        directBuf.writeBytes(dup)

        src.position(dup.position())

        val written = readableBytes - src.remaining()
        logDev(s"write() $dup $src $written")
        return written
    }

    override def close(): Unit = {
        if (opened) {
            directBuf.release()
            opened = false
        }
    }

    override def isOpen() = opened
}

class UcxDummyWritableByteChannel(val size: Int)
    extends WritableByteChannel with UcxLogging {

    protected var opened = true

    override def write(src: ByteBuffer): Int = {
        val readableBytes = src.remaining().min(size)
        val position = src.position()
        src.position(position + readableBytes)
        return readableBytes
    }

    override def close(): Unit = {
        if (opened) {
            opened = false
        }
    }

    override def isOpen() = opened
}

object UcxDummyWritableByteChannel {
    def apply(ucxWritableCh: UcxWritableByteChannel): UcxDummyWritableByteChannel = {
        new UcxDummyWritableByteChannel(ucxWritableCh.size)
    }
}