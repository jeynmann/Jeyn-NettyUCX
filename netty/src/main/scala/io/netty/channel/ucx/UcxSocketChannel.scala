
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.ByteBufAllocator
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

    protected val freeBufs = new java.util.LinkedList[ByteBuf]
    protected val readBufs = new java.util.HashMap[Int, ByteBuf]
    protected val ucxSocketConfig = new UcxSocketChannelConfig(this)
    protected var underlyingLoop: UcxEventLoop = _
    protected var underlyingUnsafe: UcxClientUnsafe = _
    protected var readSN: Int = 0
    protected var writeSN: Int = 0
    protected var writeInFlight: Int = 0
    protected var writeNonEmpty: Boolean = false

    @volatile protected var bOutputShutdown = false
    @volatile protected var bInputShutdown = false

    protected val flushWriteTask = new Runnable() {
        override
        def run(): Unit = {
            underlyingUnsafe.flush0()
        }
    }

    protected val streamProcessor = new UcxScatterMsg.Processor {
        override def accept(buf: ByteBuf, messageId: UcxScatterMsg.MessageId) = {
            doWriteByteBuf(buf)
        }
    }

    def this() = {
        this(null)
    }

    def writeComplete(buf: ByteBuf): Unit = {
        freeBufs.add(buf)
        writeInFlight -= 1
    }

    def readComplete(buf: ByteBuf, id: Int): Unit = {
        readBufs.put(id, buf)
    }

    override
    def processReady(): Unit = {
        if (!freeBufs.isEmpty()) {
            freeBufs.forEach(_.release())
            freeBufs.clear()
            if (writeNonEmpty) {
                flushWriteTask.run()
                // eventLoop().execute(flushWriteTask)
            }
        }
        if (!readBufs.isEmpty()) {
            val pipe = pipeline()
            var msg = readBufs.remove(readSN)
            while (msg != null) {
                pipe.fireChannelRead(msg)
                readSN = (readSN + 1) & Short.MaxValue
                msg = readBufs.remove(readSN)
            }
            pipe.fireChannelReadComplete()
        }
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
        var spinCount = config().getWriteSpinCount() - writeInFlight
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

        writeNonEmpty = !in.isEmpty
    }

    protected def writeScatterMessage(in: ChannelOutboundBuffer,
                                      msg: UcxScatterMsg): Int = {
        if (msg.isEmpty) {
            in.remove()
            return 0
        }

        return msg.forEachMsg(streamProcessor)
    }

    // TODO: Add sn to avoid reorder
    protected def writeByteBuf(in: ChannelOutboundBuffer, buf: ByteBuf): Int = {
        if (buf.readableBytes() == 0) {
            in.remove()
            return 0
        }
        doWriteByteBuf(buf)
        // Increment refCounts here to let processReady release
        buf.retain()
        in.remove()
        return 1
    }

    protected def doWriteByteBuf(buf: ByteBuf): Int = {
        val hasAddress = buf.hasMemoryAddress()
        val readerIndex = buf.readerIndex()
        val readableBytes = buf.readableBytes()
        val writeCb = new UcxWriteCallback().reset(this, buf, writeSN)

        if (hasAddress) {
            val address = buf.memoryAddress() + readerIndex
            underlyingUnsafe.doWrite0(address, readableBytes, writeCb)
        } else {
            val nioBuf = buf.internalNioBuffer(readerIndex, readableBytes)
            underlyingUnsafe.doWrite0(nioBuf, writeCb)
        }
        writeInFlight += 1
        writeSN = (writeSN + 1) & Short.MaxValue
        return 1
    }

    override
    protected def filterOutboundMessage(msg: Object): Object = {
        msg match {
            case buf: CompositeByteBuf => {
                if (buf.nioBufferCount == 1) {
                    UcxConverter.toDirectByteBuf(buf, config().getAllocator())
                } else {
                    new UcxCompositeByteBufMessage(buf, this)
                }
            }
            case buf: ByteBuf => {
                UcxConverter.toDirectByteBuf(buf, config().getAllocator())
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

    override
    def doHandleConnect(ep: UcpEndpoint, id: Long): Unit = {
        underlyingUnsafe.handleConnect(ep, id)
    }

    override
    def doReadAmData(ucpAmData: UcpAmData, sn: Int): Unit = {
        val readableBytes = ucpAmData.getLength.toInt

        val alloc = config().getAllocator()
        val readCb = new UcxReadCallback()
        try {
            if (ucpAmData.isDataValid()) {
                // val buf = new UcxUnsafeDirectByteBuf(
                //     alloc, ucpAmData.getDataAddress, readableBytes,
                //     _ => underlyingLoop.addAmData(ucpAmData))
                val ucpBuf = UnsafeUtils.getByteBufferView(
                    ucpAmData.getDataAddress, readableBytes)
                val buf = alloc.heapBuffer(readableBytes, readableBytes)
                buf.writeBytes(ucpBuf)
                readCb.reset(this, buf, sn)
                readCb.onSuccess(null)
            } else {
                val buf = alloc.directBuffer(readableBytes, readableBytes)
                buf.writerIndex(buf.writerIndex() + readableBytes)
                readCb.reset(this, buf, sn)
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
            opened = false
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
                close(voidPromise())
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

        private[ucx] def doWrite0(address: Long, length: Int,
                                  writeCb: UcxCallback): Int = {
            val flag = underlyingLoop.ucxStreamFlag
            val header = remoteId.directBuffer()
            header.putInt(remoteId.offset, writeSN)

            logDev(s"MESSAGE $writeSN to $remote: ongoing( $address $length)")
            ucpEp.sendAmNonBlocking(
                UcxAmId.MESSAGE, UnsafeUtils.getAddress(header), header.remaining(),
                address, length, flag, writeCb, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
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

                // local = ucpEp.getLocalAddress()
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
                        logError(s"$local CONNECT $remote: $errorMsg")
                        connectFailed(status, errorMsg)
                        header.clear()
                    }
                }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

            underlyingLoop = ucxEventLoop
            underlyingLoop.addChannel(nativeId, UcxSocketChannel.this)
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
                underlyingLoop.delChannel(ucpEp.getNativeId())
                underlyingLoop = null
                val closing = if (ucpEpAddress == null) {
                    ucpEp.closeNonBlockingForce()
                } else {
                    ucpEp.closeNonBlockingFlush()
                }
                while (!closing.isCompleted) {
                    ucpWorker.progress()
                }
                if (!freeBufs.isEmpty) {
                    freeBufs.forEach(_.release())
                    freeBufs.clear()
                }
                if (!readBufs.isEmpty()) {
                    readBufs.values().forEach(buf => {
                        if (buf.refCnt != 0) {
                            buf.release()
                        }
                    })
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

class UcxWriteCallback extends UcxCallback with UcxLogging {
    private var channel: UcxSocketChannel = _
    private var buf: ByteBuf = _
    private var sn: Int = _

    def reset(channel: UcxSocketChannel, buf: ByteBuf, sn: Int): this.type = {
        this.channel = channel
        this.buf = buf
        this.sn = sn
        this
    }

    override def onSuccess(request: UcpRequest): Unit = {
        channel.writeComplete(buf)
        logDev(s"MESSAGE $sn to ${channel.remoteAddress} success: $buf")
    }

    override def onError(status: Int, errorMsg: String): Unit = {
        channel.writeComplete(buf)
        logError(s"MESSAGE $sn to ${channel.remoteAddress} fail: $errorMsg")
        throw new UcxException(errorMsg, status)
    }
}

class UcxReadCallback extends UcxCallback with UcxLogging {
    private var channel: UcxSocketChannel = _
    private var buf: ByteBuf = _
    private var sn: Int = _

    def reset(channel: UcxSocketChannel, buf: ByteBuf, sn: Int): this.type = {
        this.channel = channel
        this.buf = buf
        this.sn = sn
        this
    }

    override def onSuccess(request: UcpRequest): Unit = {
        channel.readComplete(buf, sn)
        logDev(s"MESSAGE $sn from ${channel.remoteAddress} success: $buf")
    }

    override def onError(status: Int, errorMsg: String): Unit = {
        buf.release()
        channel.readComplete(io.netty.buffer.Unpooled.EMPTY_BUFFER, sn)
        logWarning(s"MESSAGE $sn from ${channel.remoteAddress} fail: $errorMsg")
        val e = new UcxException(errorMsg, status)
        channel.pipeline().fireChannelReadComplete().fireExceptionCaught(e)
    }
}
