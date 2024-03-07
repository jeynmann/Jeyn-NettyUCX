
package io.netty.channel.ucx

import org.openucx.jucx.ucp._

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

import io.netty.channel.unix.NativeInetAddress.address

class UcxServerSocketChannel(parent: Channel)
    extends AbstractUcxChannel(parent) with ServerSocketChannel with UcxLogging {

    protected val ucxServerConfig = new UcxServerSocketChannelConfig(this)
    protected var underlyingUnsafe: UcxServerUnsafe = _

    def this() = {
        this(null)
    }

    override
    def ucxUnsafe(): AbstractUcxUnsafe = underlyingUnsafe

    override
    def config() = ucxServerConfig

    override
    def remoteAddress(): InetSocketAddress = super.remoteAddress().asInstanceOf[InetSocketAddress]

    override
    def localAddress(): InetSocketAddress = super.localAddress().asInstanceOf[InetSocketAddress]

    override
    def parent(): ServerChannel = super.parent().asInstanceOf[ServerChannel]

    override
    protected def doBind(localAddress: SocketAddress) = {
        local = localAddress.asInstanceOf[InetSocketAddress]
        remote = null
        ucxUnsafe.setSocketAddress(local)
        ucxUnsafe.dolisten0()
        opened = true
        active = true
    }

    override
    protected def doClose(): Unit = {
        super.doClose()
        ucxUnsafe.doClose0()
    }
    
    protected def newChildChannel(epIn: UcpEndpoint): UcxSocketChannel = {
        val child = new UcxSocketChannel(this)
        child.ucxUnsafe.doAccept0(epIn)
        child
    }

    override
    protected def newUnsafe(): AbstractUcxUnsafe = {
        underlyingUnsafe = new UcxServerUnsafe()
        underlyingUnsafe
    }

    class UcxServerUnsafe extends AbstractUcxUnsafe {
        val ucpConnectHandler = new UcpListenerConnectionHandler {
            override def onConnectionRequest(request: UcpConnectionRequest) = {
                val id = request.getClientId
                val address = request.getClientAddress

                setConnectionRequest(request)
                setErrorHandler(ucpErrHandler(s"Endpoint to $address"))

                val epIn = ucpWorker.newEndpoint(ucpEpParam)
                pipeline.fireChannelRead(newChildChannel(epIn))
            }
        }

        val ucpListenerParam = new UcpListenerParams().setConnectionHandler(ucpConnectHandler)
        var ucpListener: UcpListener = _
        var ucpSocketAddress: InetSocketAddress = _

        protected def ucpWorker = ucxEventLoop.ucpWorker

        protected def setConnectionRequest(ucpConnectionRequest: UcpConnectionRequest): Unit = {
            ucpEpParam.setConnectionRequest(ucpConnectionRequest)
        }

        protected def setErrorHandler(errHandler: UcpEndpointErrorHandler): Unit = {
            ucpEpParam.setPeerErrorHandlingMode().setErrorHandler(errHandler)
        }

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
            ucpSocketAddress = address
        }
        override
        def dolisten0():Unit = {
            ucpListener = ucpWorker.newListener(ucpListenerParam)
        }

        override
        def doClose0():Unit = {
            if (ucpListener != null) {
                ucpListener.close()
                ucpListener = null
            }
        }
    }
}