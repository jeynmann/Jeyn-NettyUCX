
package io.netty.channel.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelPromise
import io.netty.channel.ChannelOutboundBuffer
import io.netty.channel.EventLoop
import io.netty.channel.ServerChannel
import io.netty.channel.socket.ServerSocketChannel

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.Collection
import java.util.Collections
import java.util.Map

class UcxServerSocketChannel(parent: Channel)
    extends AbstractUcxChannel(parent) with ServerSocketChannel with UcxLogging {
    logDev(s"UcxServerSocketChannel()")

    protected val ucxServerConfig = new UcxServerSocketChannelConfig(this)
    protected var underlyingUnsafe: UcxServerUnsafe = _

    def this() = {
        this(null)
    }

    override
    def ucxUnsafe: AbstractUcxUnsafe = underlyingUnsafe

    override
    def config(): UcxServerSocketChannelConfig = ucxServerConfig

    override
    def remoteAddress(): InetSocketAddress = super.remoteAddress().asInstanceOf[InetSocketAddress]

    override
    def localAddress(): InetSocketAddress = super.localAddress().asInstanceOf[InetSocketAddress]

    override
    def parent(): ServerChannel = super.parent().asInstanceOf[ServerChannel]

    override
    protected def doBind(localAddress: SocketAddress) = {
        logDev(s"doBind() $localAddress <- _")
        // Called inside AbstractUnsafe.bind.try in eventLoop
        ucxUnsafe.setSocketAddress(localAddress.asInstanceOf[InetSocketAddress])
        ucxUnsafe.dolisten0()
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

    protected def newChildChannel(): UcxSocketChannel = {
        new UcxSocketChannel(this)
    }

    override
    protected def newUnsafe(): AbstractUcxUnsafe = {
        logDev(s"newUnsafe()")
        underlyingUnsafe = new UcxServerUnsafe()
        underlyingUnsafe
    }

    class UcxServerUnsafe extends AbstractUcxUnsafe {
        val ucpConnectHandler = new UcpListenerConnectionHandler {
            override def onConnectionRequest(request: UcpConnectionRequest) = {
                val childChannel = newChildChannel()
                childChannel.ucxUnsafe.setConnectionRequest(request)
                pipeline.fireChannelRead(childChannel)
            }
        }

        val ucpListenerParam = new UcpListenerParams().setConnectionHandler(ucpConnectHandler)
        var ucpListener: UcpListener = _

        override
        def connect(
            remoteAddress: SocketAddress, localAddress: SocketAddress,
            promise: ChannelPromise): Unit = {
            // Connect not supported by ServerChannel implementations
            promise.setFailure(new UnsupportedOperationException())
        }

        override
        def setSocketAddress(address: InetSocketAddress): Unit = {
            ucpListenerParam.setSockAddr(address)
        }

        override
        def dolisten0():Unit = {
            ucpListener = ucpWorker.newListener(ucpListenerParam)
            local = ucpListener.getAddress()

            val nativeId = ucpListener.getNativeId()
            uniqueId.set(nativeId)
            ucxEventLoop.addChannel(nativeId, UcxServerSocketChannel.this)
            active = true
        }

        override
        def doClose0():Unit = {
            if (ucpListener != null) {
                logDev(s"doClose0()")
                ucxEventLoop.delChannel(ucpListener.getNativeId())
                ucpListener.close()
                ucpListener = null
            }
        }
    }
}