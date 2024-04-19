
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
// import io.netty.buffer.UcxUnsafeDirectByteBuf
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

    protected val readBufs = new java.util.LinkedList[ByteBuf]
    protected val ucxSocketConfig = new UcxSocketChannelConfig(this)
    protected var underlyingUnsafe: UcxClientUnsafe = _

    @volatile protected var bOutputShutdown = false
    @volatile protected var bInputShutdown = false

    protected val flushWriteTask = new Runnable() {
        override
        def run(): Unit = {
            underlyingUnsafe.flush0()
        }
    }

    protected val messageProcessor = new UcxScatterMsg.Processor {
        override def accept(buf: ByteBuf, isCompleted: Boolean) = {
            doWriteByteBuf(buf, isCompleted)
        }
    }

    protected def flushReadBufs(): Unit = {
        var buf: ByteBuf = null
        val frameNums = readBufs.size()
        if (frameNums == 1) {
            buf = readBufs.getFirst()
        } else {
            buf = new CompositeByteBuf(config().getAllocator(), true, frameNums, readBufs)
        }
        pipeline().fireChannelRead(buf).fireChannelReadComplete()
        logDev(s"Read MESSAGE from $remote success: $buf")
        readBufs.clear()
    }

    protected def flushReadBufs(status: Int, errorMsg: String): Unit = {
        readBufs.forEach(_.release())
        readBufs.clear()
        val e = new UcxException(s"Read MESSAGE from $remote fail: $errorMsg", status)
        pipeline().fireChannelReadComplete().fireExceptionCaught(e)
    }

    protected val emptyReadCallback = new UcxCallback() {
        override def onSuccess(r: UcpRequest): Unit = {}
        override def onError(status: Int, errorMsg: String): Unit = {
            flushReadBufs(status, errorMsg)
        }
    }

    protected val defaultReadCallback = new UcxCallback() {
        override def onSuccess(r: UcpRequest): Unit = {
            flushReadBufs()
        }
        override def onError(status: Int, errorMsg: String): Unit = {
            flushReadBufs(status, errorMsg)
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
        var spinCount = config().getWriteSpinCount()
        while (!in.isEmpty() && spinCount > 0) {
            val msg = in.current()
            msg match {
            case buf: ByteBuf =>
                spinCount -= writeByteBuf(in, buf)
            case scm: UcxScatterMsg =>
                spinCount -= writeScatterMessage(in, scm)
            case _ =>
                // Should never reach here.
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
            }
        }

        if (!in.isEmpty()) {
            eventLoop().execute(flushWriteTask)
        }
    }

    protected def writeScatterMessage(in: ChannelOutboundBuffer,
                                      msg: UcxScatterMsg): Int = {
        if (msg.isEmpty) {
            in.remove()
            return 0
        }

        return msg.forEachMsg(messageProcessor)
    }

    // TODO: Add sn to avoid reorder
    protected def writeByteBuf(in: ChannelOutboundBuffer, buf: ByteBuf): Int = {
        if (buf.readableBytes() == 0) {
            in.remove()
            return 0
        }
        // Increment refCounts here to let UcxCallback release
        buf.retain()

        val spinNum = doWriteByteBuf(buf, true)
        in.remove()
        return spinNum
    }

    protected def doWriteByteBuf(buf: ByteBuf, isCompleted: Boolean): Int = {
        val nioBufferCount = buf.nioBufferCount()
        val writeCb = newWriteUcxCallback(buf, nioBufferCount)

        val hasAddress = buf.hasMemoryAddress()
        val writeOnce = hasAddress || (nioBufferCount == 1)
        val readerIndex = buf.readerIndex()
        val readableBytes = buf.readableBytes()
        val amId = if (isCompleted) UcxAmId.MESSAGE else UcxAmId.MESSAGE_MIDDLE
        if (writeOnce) {
            if (hasAddress) {
                val address = buf.memoryAddress() + readerIndex
                underlyingUnsafe.doWrite0(amId, address, readableBytes, writeCb)
            } else {
                val nioBuf = buf.internalNioBuffer(readerIndex, readableBytes)
                underlyingUnsafe.doWrite0(amId, nioBuf, writeCb)
            }
        } else {
            val nioBuffers = buf.nioBuffers(readerIndex, readableBytes)
            val tailId = nioBufferCount - 1
            var i = 0
            do {
                val midBuf = nioBuffers(i)
                underlyingUnsafe.doWrite0(UcxAmId.MESSAGE_MIDDLE, midBuf, writeCb)
                i += 1
            } while (i != tailId)
            val tailBuf = nioBuffers(tailId)
            underlyingUnsafe.doWrite0(amId, tailBuf, writeCb)
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
                new UcxDefaultFileRegionMessage(fr, this)
            }
            case fr: FileRegion => {
                new UcxFileRegionMessage(fr, this)
            }
            case m: UcxScatterMsg => m
            case _ => 
                throw new UnsupportedOperationException(
                    s"unsupported message type: ${msg.getClass}")
        }
    }

    protected def newWriteUcxCallback(buf: ByteBuf, refCounts: Int): UcxCallback = {
        new UcxSharedCallback(buf, refCounts, local, remote)
    }

    override
    def doReadAmData(ucpAmData: UcpAmData, isCompleted: Boolean): Unit = {
        val readableBytes = ucpAmData.getLength.toInt
        val pipe = pipeline()

        val alloc = config().getAllocator()
        val readCb = if (isCompleted) defaultReadCallback else emptyReadCallback
        try {
            if (ucpAmData.isDataValid()) {
                val ucpBuf = UnsafeUtils.getByteBufferView(
                    ucpAmData.getDataAddress, readableBytes)
                val buf = alloc.heapBuffer(readableBytes, readableBytes)
                buf.writeBytes(ucpBuf)
                readBufs.add(buf)
                readCb.onSuccess(null)
            } else {
                val buf = alloc.directBuffer(readableBytes, readableBytes)
                buf.writerIndex(buf.writerIndex() + readableBytes)
                readBufs.add(buf)
                underlyingUnsafe.doRead0(ucpAmData, buf, readCb) 
            }
        } catch {
            case t: Throwable => {
                readCb.onError(-1, t.toString())
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

        private[ucx] def doWrite0(amId: Int, buf: ByteBuffer,
                                  writeCb: UcxCallback): Int = {
            val address = UnsafeUtils.getAddress(buf) + buf.position()
            val length = buf.remaining()
            doWrite0(amId, address, length, writeCb)
        }

        private[ucx] def doWrite0(amId: Int, address: Long, length: Int,
                                  writeCb: UcxCallback): Int = {
            val header = remoteId.directBuffer()

            logDev(s"$local MESSAGE $remote: ongoing($amId $address $length)")
            ucpEp.sendAmNonBlocking(
                amId, UnsafeUtils.getAddress(header), header.remaining(),
                address, length, 0, writeCb, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
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
                if (!readBufs.isEmpty()) {
                    readBufs.forEach(_.release())
                    readBufs.clear()
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
