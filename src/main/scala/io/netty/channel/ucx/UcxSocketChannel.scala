
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelPromise
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion
import io.netty.channel.socket.SocketChannel

import java.nio.ByteBuffer
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

    protected val streamStates = new scala.collection.mutable.HashMap[Int, StreamState]
    @volatile protected var bOutputShutdown = false
    @volatile protected var bInputShutdown = false

    protected val flushTask = new Runnable() {
        override
        def run(): Unit = {
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
        var spinLimit = config().getWriteSpinCount()
        while (!in.isEmpty() && spinLimit > 0) {
            val msg = in.current()
            msg match {
            case buf: ByteBuf =>
                spinLimit -= writeByteBuf(in, buf)
            case fm: UcxFileRegionMsg =>
                spinLimit -= writeFileMessage(in, fm, spinLimit)
            case _ =>
                // Should never reach here.
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
            }
        }

        if (!in.isEmpty()) {
            eventLoop().execute(flushTask)
        }
    }

    class UcxDebugCallback(doComplete: () => Unit)
        extends UcxCallback with UcxLogging {
        override def onSuccess(request: UcpRequest): Unit = {
            doComplete()
            logDev(s"$local MESSAGE $remote: success")
        }

        override def onError(status: Int, errorMsg: String): Unit = {
            doComplete()
            throw new UcxException(s"$local MESSAGE $remote: $errorMsg", status)
        }
    }

    protected def writeFileMessage(in: ChannelOutboundBuffer, fm: UcxFileRegionMsg,
                                   spinLimit: Int): Int = {
        var headerBuf: ByteBuf = null
        try {
            val frameNums = fm.frameNums
            if (frameNums == 1) {
                fm.forall(doWriteByteBuf _)
                in.remove()
                return 1
            }

            val headerSize = UnsafeUtils.LONG_SIZE + UnsafeUtils.INT_SIZE +
                             UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE

            headerBuf = UcxPooledByteBufAllocator.directBuffer(
                config().getAllocator(), headerSize, headerSize)

            val streamId = StreamState.nextId
            val nioBuf = headerBuf.internalNioBuffer(headerBuf.readerIndex(),
                                                     headerSize).slice()

            nioBuf.putLong(underlyingUnsafe.remoteId.get)
            nioBuf.putInt(streamId)
            nioBuf.putInt(frameNums)

            val frameIdPos = headerSize - UnsafeUtils.INT_SIZE
            def processor(frameId: Int, buf: ByteBuf): Unit = {
                val writeCb = newFrameUcxCallback(buf)
                val body = buf.internalNioBuffer(buf.readerIndex(), buf.readableBytes())
                nioBuf.position(frameIdPos)
                nioBuf.putInt(frameId).rewind()
                underlyingUnsafe.doWriteFrame0(nioBuf, body, writeCb)
            }
            val spinNum = fm.forEachFrame(processor, spinLimit)

            if (fm.isEmpty) {
                in.remove()
            }
            return spinNum
        } finally {
            if (headerBuf != null) {
                headerBuf.release()
            }
        }
    }

    protected def writeByteBuf(in: ChannelOutboundBuffer, buf: ByteBuf): Int = {
        if (buf.readableBytes() == 0) {
            in.remove()
            return 0
        }
        // Increment refCounts here to let UcxCallback release
        buf.retain()

        val spinNum = doWriteByteBuf(buf)
        in.remove()
        return spinNum
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
                val address = buf.memoryAddress() + readerIndex
                underlyingUnsafe.doWrite0(address, readableBytes, writeCb)
            } else {
                val nioBuf = buf.internalNioBuffer(readerIndex, readableBytes)
                underlyingUnsafe.doWrite0(nioBuf, writeCb)
            }
        } else {
            buf.nioBuffers(readerIndex, readableBytes).foreach(
                underlyingUnsafe.doWrite0(_, writeCb))
        }

        return nioBufferCount
    }

    override
    protected def filterOutboundMessage(msg: Object): Object = {
        msg match {
            case buf: ByteBuf => {
                toDirectBuffer(buf)
            }
            case fr: DefaultFileRegion => {
                if (fr.count() == fr.transferred()) {
                    io.netty.buffer.Unpooled.EMPTY_BUFFER
                } else {
                    new UcxDefaultFileRegionMsg(fr, this)
                }
            }
            case fr: FileRegion => {
                if (fr.count() == fr.transferred()) {
                    io.netty.buffer.Unpooled.EMPTY_BUFFER
                } else {
                    new UcxFileRegionMsg(fr, this)
                }
            }
            case _ => 
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
        }
    }

    protected def newWriteUcxCallback(buf: ByteBuf, refCounts: Int): UcxCallback = {
        new UcxSharedCallback(buf, refCounts, local, remote)
    }

    protected def newFrameUcxCallback(buf: ByteBuf): UcxCallback = {
        new UcxFrameCallback(buf, local, remote)
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
    def doReadStream(ucpAmData: UcpAmData, streamId: Int, frameNum: Int, frameId: Int): Unit = {
        val readableBytes = ucpAmData.getLength.toInt
        var streamState: StreamState = null

        try {
            streamState = streamStates.getOrElseUpdate(streamId, {
                new StreamState(this, streamId, frameNum,
                                _ => streamStates.remove(streamId))
            })

            val directBuf = streamState.frameBuf(frameId, readableBytes)

            if (ucpAmData.isDataValid()) {
                val ucpBuf = UnsafeUtils.getByteBufferView(
                    ucpAmData.getDataAddress, readableBytes)
                directBuf.writeBytes(ucpBuf)
                streamState.onSuccess(null)
            } else {
                directBuf.writerIndex(directBuf.writerIndex() + readableBytes)
                underlyingUnsafe.doRead0(ucpAmData, directBuf, streamState) 
            }
        } catch {
            case t: Throwable => {
                if (streamState != null) {
                    streamState.onError(-1, t.toString())
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
            val address = UnsafeUtils.getAddress(buf) + buf.position()
            val length = buf.remaining()
            doWrite0(address, length, writeCb)
        }

        private[ucx] def doWrite0(address: Long, length: Int, writeCb: UcxCallback): Int = {
            val header = remoteId.directBuffer()

            logDev(s"$local MESSAGE $remote: ongoing($address $length)")
            ucpEp.sendAmNonBlocking(
                UcxAmId.MESSAGE,
                UnsafeUtils.getAddress(header), header.remaining(),
                address, length, 0, writeCb, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            return 1
        }

        private[ucx] def doWriteFrame0(header: ByteBuffer, buf: ByteBuffer, 
                                       writeCb: UcxCallback): Int = {
            val headerAddress = UnsafeUtils.getAddress(header) + header.position()
            val headerLength = header.remaining()
            val address = UnsafeUtils.getAddress(buf) + buf.position()
            val length = buf.remaining()

            // TODO UCP_AM_SEND_FLAG_COPY_HEADER
            logDev(s"$local STREAM $remote: ongoing($address $length)")

            ucpEp.sendAmNonBlocking(
                UcxAmId.STREAM,
                headerAddress, headerLength, address, length, 8, writeCb,
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

class UcxFrameCallback(buf: ByteBuf, local: SocketAddress,
                       remote: SocketAddress)
    extends UcxCallback with UcxLogging {
    override def onSuccess(request: UcpRequest): Unit = {
        buf.release()
    }

    override def onError(status: Int, errorMsg: String): Unit = {
        buf.release()
        throw new UcxException(s"$local STREAM $remote: $errorMsg", status)
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

class StreamState(ucxChannel: UcxSocketChannel, streamId: Int, frameNum: Int,
                  onComplete: ByteBuf => Unit)
    extends UcxCallback with UcxLogging {

    private[this] val alloc = ucxChannel.config().getAllocator()
    private[this] val remote = ucxChannel.remoteAddress()

    private[this] val framesBuf = new Array[ByteBuf](frameNum)
    private[this] var received = 0

    def frameBuf(frameId: Int, realSize: Int): ByteBuf = {
        val frameNow = UcxPooledByteBufAllocator.directBuffer(alloc, realSize, realSize)
        framesBuf(frameId) = frameNow
        frameNow
    }

    override def onSuccess(r: UcpRequest): Unit = {
        received += 1
        if (received == frameNum) {
            val buf = new CompositeByteBuf(alloc, true, frameNum)
            framesBuf.foreach(buf.addComponent(true, _))
            logDev(s"Read STREAM from $remote success: ($streamId-$received ${buf.readableBytes})")
            ucxChannel.pipeline().fireChannelRead(buf).fireChannelReadComplete()
            onComplete(buf)
        }
    }

    override def onError(status: Int, errorMsg: String): Unit = {
        val e = new UcxException(s"Read STREAM from $remote fail: ($streamId-$received $errorMsg)", status)
        onComplete(null)
        framesBuf.foreach(_.release())
        ucxChannel.pipeline().fireChannelReadComplete().fireExceptionCaught(e)
    }
}

object StreamState {
    val seed = new java.util.Random
    val streamId = new java.util.concurrent.atomic.AtomicInteger(seed.nextInt)

    def nextId = streamId.incrementAndGet()
}