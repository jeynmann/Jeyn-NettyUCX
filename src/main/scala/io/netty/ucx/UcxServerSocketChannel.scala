
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
        logDev(s"newChildChannel()")
        new UcxSocketChannel(this)
    }

    override
    protected def newUnsafe(): AbstractUcxUnsafe = {
        logDev(s"newUnsafe()")
        underlyingUnsafe = new UcxServerUnsafe()
        underlyingUnsafe
    }

    class UcxServerUnsafe extends AbstractUcxUnsafe {
        logDev(s"UcxServerUnsafe()")
        val ucpConnectHandler = new UcpListenerConnectionHandler {
            override def onConnectionRequest(request: UcpConnectionRequest) = {
                setConnectionRequest(request)
                doAccept0()
            }
        }

        val ucpListenerParam = new UcpListenerParams().setConnectionHandler(ucpConnectHandler)
        var ucpListener: UcpListener = _

        val ucpErrHandler = new UcpEndpointErrorHandler() {
            override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
                if (status == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                    logInfo(s"$ep: $errorMsg")
                } else {
                    logWarning(s"$ep: $errorMsg")
                }
                Option(ucxEventLoop.delChannel(ep.getNativeId())).foreach(ch =>
                    ch.ucxUnsafe.close(ch.ucxUnsafe.voidPromise()))
            }
        }
        val ucpEpParam = new UcpEndpointParams()

        override
        def connect(
            remoteAddress: SocketAddress, localAddress: SocketAddress,
            promise: ChannelPromise): Unit = {
            logDev(s"connect() $remoteAddress $localAddress")
            // Connect not supported by ServerChannel implementations
            promise.setFailure(new UnsupportedOperationException())
        }

        override
        def setConnectionRequest(request: UcpConnectionRequest): Unit = {
            val address = request.getClientAddress()
            ucpEpParam.setConnectionRequest(request)
                .setPeerErrorHandlingMode()
                .setErrorHandler(ucpErrHandler)
                .setName(s"Ep from ${address}")
        }

        override
        def setSocketAddress(address: InetSocketAddress): Unit = {
            logDev(s"setSocketAddress() $address")
            ucpListenerParam.setSockAddr(address)
            local = address
        }

        override
        def dolisten0():Unit = {
            logDev(s"dolisten0()")
            ucpListener = ucpWorker.newListener(ucpListenerParam)
            active = true
        }

        override
        def doAccept0(): Unit = {
            val childChannel = newChildChannel()
            try {
                val ep = ucpWorker.newEndpoint(ucpEpParam)

                childChannel.ucxUnsafe.setUcpEp(ep)
                ucxEventLoop.addChannel(ep.getNativeId(), childChannel)
                pipeline.fireChannelRead(childChannel)

                logDebug(s"accept $local <- $remote")
            } catch {
                case e: Throwable => childChannel.ucxUnsafe.connectFailed(
                    UcsConstants.STATUS.UCS_ERR_IO_ERROR, "accept $local <- $remote: $e")
            }
        }

        override
        def doClose0():Unit = {
            logDev(s"doClose0()")
            if (ucpListener != null) {
                ucpListener.close()
                ucpListener = null
            }
        }
    }
}