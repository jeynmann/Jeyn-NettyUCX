package io.netty.channel.ucx

import org.openucx.jucx.ucp._

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import io.netty.channel.AbstractChannel
import io.netty.channel.Channel
import io.netty.channel.ChannelConfig
import io.netty.channel.ChannelException
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelMetadata
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.ChannelPromise
import io.netty.channel.ConnectTimeoutException
import io.netty.channel.EventLoop
import io.netty.channel.RecvByteBufAllocator.ExtendedHandle
import io.netty.channel.socket.ChannelInputShutdownEvent
import io.netty.channel.socket.ChannelInputShutdownReadComplete
import io.netty.channel.socket.SocketChannelConfig
import io.netty.channel.unix.FileDescriptor
import io.netty.channel.unix.Socket
import io.netty.channel.unix.UnixChannel
import io.netty.util.ReferenceCountUtil

import java.io.IOException
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AlreadyConnectedException
import java.nio.channels.ClosedChannelException
import java.nio.channels.ConnectionPendingException
import java.nio.channels.NotYetConnectedException
import java.nio.channels.UnresolvedAddressException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

import io.netty.channel.internal.ChannelUtils.WRITE_STATUS_SNDBUF_FULL
import io.netty.channel.unix.UnixChannelUtil.computeRemoteAddr
import io.netty.util.internal.ObjectUtil.checkNotNull

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

abstract class AbstractUcxChannel(parent: Channel) extends AbstractChannel(parent) with UcxLogging {

    protected def ucxEventLoop(): UcxEventLoop = eventLoop().asInstanceOf[UcxEventLoop]

    protected def eventLoopRun(fn: () => Unit):Unit = {
        val loop = eventLoop()
        if (loop.inEventLoop()) {
            fn()
        } else {
            loop.execute(() => fn())
        }
    }

    /**
     * The future of the current connection attempt.  If not null, subsequent
     * connection attempts will fail.
     */
    protected var connectPromise: ChannelPromise = _
    protected var connectTimeoutFuture: ScheduledFuture[Any] = _

    @volatile protected var local: InetSocketAddress = _
    @volatile protected var remote: InetSocketAddress = _
    @volatile protected var active: Boolean = false
    @volatile protected var opened: Boolean = false

    def this() = {
        this(null)
    }

    // Ucx apis
    def ucxUnsafe(): AbstractUcxUnsafe = ???

    def newDirectBuffer( buf: ByteBuf): ByteBuf = {
        newDirectBuffer(buf, buf)
    }

    def newDirectBuffer(holder: Object, buf: ByteBuf): ByteBuf = {
        val readableBytes = buf.readableBytes()
        if (readableBytes == 0) {
            ReferenceCountUtil.release(holder)
            return Unpooled.EMPTY_BUFFER
        }

        val allocator = alloc()
        if (allocator.isDirectBufferPooled()) {
            return newDirectBuffer0(holder, buf, allocator, readableBytes)
        }

        val directBuf = ByteBufUtil.threadLocalDirectBuffer()
        if (directBuf == null) {
            return newDirectBuffer0(holder, buf, allocator, readableBytes)
        }

        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes)
        ReferenceCountUtil.safeRelease(holder)
        return directBuf
    }

    private def newDirectBuffer0(
        holder: Object, buf: ByteBuf, allocator: ByteBufAllocator,
        capacity: Int): ByteBuf = {
        val directBuf = allocator.directBuffer(capacity)
        directBuf.writeBytes(buf, buf.readerIndex(), capacity)
        ReferenceCountUtil.safeRelease(holder)
        return directBuf
    }

    def ucxRead(ucpAmData: UcpAmData): Unit = doReadAmData(ucpAmData)

    def ucxConnectBack(remoteId: Long, address: ByteBuffer): Unit = {
        ucxUnsafe.remoteId.set(remoteId)
        ucxUnsafe.setUcpAddress(address)
        ucxUnsafe.doConnectBack0()
    }

    def ucxConnectDone(remoteId: Long): Unit = {
        ucxUnsafe.remoteId.set(remoteId)
        ucxUnsafe.doConnectDone0()
    }

    override
    def config(): UcxChannelConfig = ???

    override
    def metadata() = AbstractUcxChannel.METADATA

    override
    def isActive() = active

    override
    def isOpen() = opened

    override
    protected def newUnsafe(): AbstractUnsafe = ???

    protected abstract class AbstractUcxUnsafe extends AbstractUnsafe {
        val uniqueId = new NettyUcxId()
        val remoteId = new NettyUcxId()
        val ucpEpParam = new UcpEndpointParams()

        private var allocHandle: UcxRecvByteAllocatorHandle = _

        protected def ucpErrHandler(msg: => String) = new UcpEndpointErrorHandler() {
            override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
                logError(s"$msg: $ep => $errorMsg")
            }
        }

        override
        def recvBufAllocHandle(): UcxRecvByteAllocatorHandle = {
            if (allocHandle == null) {
                allocHandle = new UcxRecvByteAllocatorHandle(
                    super.recvBufAllocHandle().asInstanceOf[ExtendedHandle])
            }
            allocHandle
        }

        override
        protected def flush0(): Unit = {
            // PASS
        }

        override
        def connect(
            remoteAddress: SocketAddress, localAddress: SocketAddress, promise: ChannelPromise): Unit = {
            if (!promise.setUncancellable() || !ensureOpen(promise)) {
                return
            }

            try {
                if (connectPromise != null) {
                    throw new ConnectionPendingException()
                }

                connectPromise = promise

                doConnect(remoteAddress, localAddress)

                // Schedule connect timeout.
                val connectTimeoutMillis = config().getConnectTimeoutMillis()
                if (connectTimeoutMillis > 0) {
                    connectTimeoutFuture = eventLoop().schedule(() => {
                        val cause = new ConnectTimeoutException("connection timed out: " + remoteAddress)
                        if (connectPromise != null && connectPromise.tryFailure(cause)) {
                            close(voidPromise())
                        }
                    }
                    , connectTimeoutMillis, TimeUnit.MILLISECONDS)
                }

                promise.addListener(new ChannelFutureListener() {
                    override
                    def operationComplete(future: ChannelFuture) = {
                        if (future.isCancelled()) {
                            if (connectTimeoutFuture != null) {
                                connectTimeoutFuture.cancel(false)
                            }
                            connectPromise = null
                            close(voidPromise())
                        }
                    }
                })
            } catch {
                case t: Throwable => {
                    closeIfClosed()
                    promise.tryFailure(annotateConnectException(t, remoteAddress))
                }
            }
        }

        protected def finishConnect(remoteAddress: SocketAddress): Unit = {
            // Note this method is invoked by the event loop only if the connection attempt was
            // neither cancelled nor timed out.

            assert(eventLoop().inEventLoop())

            try {
                val wasActive = isActive()
                fulfillConnectPromise(connectPromise, wasActive)
                if (connectTimeoutFuture != null) {
                    connectTimeoutFuture.cancel(false)
                }
                connectPromise = null
            } catch {
                case t: Throwable =>
                    fulfillConnectPromise(connectPromise, annotateConnectException(t, remoteAddress))
            }
        }

        private def fulfillConnectPromise(promise: ChannelPromise, wasActive: Boolean): Unit = {
            if (promise == null) {
                // Closed via cancellation and the promise has been notified already.
                return
            }
            active = true
        
            // Get the state as trySuccess() may trigger an ChannelFutureListener that will close the Channel.
            // We still need to ensure we call fireChannelActive() in this case.
            val nowActive = isActive()
        
            // trySuccess() will return false if a user cancelled the connection attempt.
            val promiseSet = promise.trySuccess()
        
            // Regardless if the connection attempt was cancelled, channelActive() event should be triggered,
            // because what happened is what happened.
            if (!wasActive && nowActive) {
                pipeline().fireChannelActive()
            }
        
            // If a user cancelled the connection attempt, close the channel, which is followed by channelInactive().
            if (!promiseSet) {
                close(voidPromise())
            }
        }

        private def fulfillConnectPromise(promise: ChannelPromise, cause: Throwable): Unit = {
            if (promise == null) {
                // Closed via cancellation and the promise has been notified already.
                return
            }
        
            // Use tryFailure() instead of setFailure() to avoid the race against cancel().
            promise.tryFailure(cause)
            closeIfClosed()
        }

        def setSocketAddress(address: InetSocketAddress): Unit = {
            throw new UnsupportedOperationException()
        }

        def setUcpAddress(address: ByteBuffer): Unit = {
            throw new UnsupportedOperationException()
        }

        def dolisten0(): Unit = {
            throw new UnsupportedOperationException()
        }

        def doConnect0(): Unit = {
            throw new UnsupportedOperationException()
        }

        def doConnectDone0(): Unit = {
            throw new UnsupportedOperationException()
        }

        def doAccept0(epIn: UcpEndpoint): Unit = {
            throw new UnsupportedOperationException()
        }

        def doConnectBack0(): Unit = {
            throw new UnsupportedOperationException()
        }

        def doConnectedBackDone0(): Unit = {
            throw new UnsupportedOperationException()
        }

        def doClose0(): Unit = {
            throw new UnsupportedOperationException()
        }
    }

    override
    protected def isCompatible(loop: EventLoop) = loop.isInstanceOf[UcxEventLoop]

    override
    protected def localAddress0(): SocketAddress = {
        return local
    }

    override
    protected def remoteAddress0(): SocketAddress = {
        return remote
    }

    override
    protected def doClose(): Unit = {
        active = false
        try {
            val promise = connectPromise
            if (promise != null) {
                // Use tryFailure() instead of setFailure() to avoid the race against cancel().
                promise.tryFailure(new ClosedChannelException())
                connectPromise = null
            }

            val future = connectTimeoutFuture
            if (future != null) {
                future.cancel(false)
                connectTimeoutFuture = null
            }

            if (isRegistered()) {
                eventLoopRun(doDeregister _)
            }
        } finally {
        }
    }

    override
    protected def doRegister(): Unit = {
        ucxEventLoop.addChannel(this)
    }

    override
    protected def doDeregister(): Unit = {
        ucxEventLoop.delChannel(this)
    }

    override
    protected def doBind(local: SocketAddress): Unit = {
        throw new UnsupportedOperationException()
    }

    override
    protected def doDisconnect(): Unit = {
        doClose()
    }

    override
    protected final def doBeginRead(): Unit = {
    }

    override
    protected def doWrite(in: ChannelOutboundBuffer): Unit = {
        throw new UnsupportedOperationException()
    }

    override
    protected def filterOutboundMessage(msg: Object): Object = {
        throw new UnsupportedOperationException()
    }

    protected def doReadAmData(ucpAmData: UcpAmData): Unit = {
        throw new UnsupportedOperationException()
    }

    protected def doConnect(remoteAddress: SocketAddress, localAddress: SocketAddress): Unit = {
        throw new UnsupportedOperationException()
    }

}

object AbstractUcxChannel {
    final val METADATA = new ChannelMetadata(false)
    final val SNDBUF_FULL = Integer.MAX_VALUE // TODO: ???
}