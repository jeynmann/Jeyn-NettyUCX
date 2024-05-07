package io.netty.channel.ucx.examples

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap

import io.netty.channel.ucx._
import io.netty.channel.Channel
import io.netty.channel.ChannelOption
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.util.concurrent.GenericFutureListener

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.Scanner

class EchoDemo(
    isServer: Boolean,
    bindAddress: InetSocketAddress, remoteAddress: InetSocketAddress)
    extends Runnable {

    private final val LOGGER = LoggerFactory.getLogger(classOf[EchoDemo])

    def this() = {
        this(true, new InetSocketAddress(EchoDemo.DEFAULT_SERVER_PORT), null)
    }

    def this(bindAddress: InetSocketAddress) = {
        this(true, bindAddress, null)
    }

    def this(bindAddress: InetSocketAddress, remoteAddress: InetSocketAddress) = {
        this(false, bindAddress, remoteAddress)
    }

    override
    def run(): Unit = {
        if (!isServer && remoteAddress == null) {
            LOGGER.error("Please specify the server address")
            return
        }

        val runnable = if (isServer) new Server(bindAddress) else new Client(bindAddress, remoteAddress)
        runnable.run()
    }
}

object EchoDemo {
    private final val DEFAULT_SERVER_PORT = 2998

    def main(args: Array[String]): Unit = {
        initializeLogging()
        EchoDemo(args).run()
    }

    def apply(args: Array[String]): EchoDemo = {  
        args.length match {
            case 0 => new EchoDemo()
            case 1 => new EchoDemo(new InetSocketAddress(args(0).toInt)) // port <-
            case 2 => new EchoDemo(null, new InetSocketAddress(args(0), args(1).toInt)) // -> host port
            case 3 => new EchoDemo(new InetSocketAddress(args(0).toInt), new InetSocketAddress(args(1), args(2).toInt)) // port -> host port
            case _ => new EchoDemo(new InetSocketAddress(args(0), args(1).toInt), new InetSocketAddress(args(2), args(3).toInt)) // host port -> host port
        }
    }

    def initializeLogging(): Unit = {
        val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
        // scalastyle:off println
        if (!log4j12Initialized) {
            val url = new java.io.File("log4j.properties").toURI().toURL()
            PropertyConfigurator.configure(url)
            println(s"Using Demo's default log4j profile: $url")
        }
  
        val rootLogger = LogManager.getRootLogger()
        rootLogger.getLevel()
    }
}

class Client(
    bindAddress: InetSocketAddress, remoteAddress: InetSocketAddress) extends Runnable {

    private final val WORKER_THREADS = 1
    private final val LOGGER = LoggerFactory.getLogger(classOf[Client])

    private final var scanner: Scanner  = new Scanner(System.in)

    override
    def run() = {
        LOGGER.info("Connecting to server [{}]", remoteAddress)
        val workerGroup = new UcxEventLoopGroup(WORKER_THREADS)
        val bootstrap = new Bootstrap()

        bootstrap.group(workerGroup)
            .channel(classOf[UcxSocketChannel])
            .handler(new ChannelInitializer[SocketChannel]() {
                override
                protected def initChannel(channel: SocketChannel) = {
                    channel.pipeline().addLast("echo", new Handler())
                }
            })
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS.asInstanceOf[ChannelOption[Any]], 5000)

        try {
            val channel = bootstrap.connect(remoteAddress, bindAddress).sync().channel()
            val scanThread = new Thread(() => {
                while (scanner.hasNextLine()) {
                    try {
                        val line = scanner.nextLine()
                        val msg = Unpooled.copiedBuffer(line, StandardCharsets.UTF_8)
                        LOGGER.debug("[input] {}", msg)
                        channel.writeAndFlush(msg).sync()
                    } catch {
                        case e: InterruptedException =>
                        LOGGER.error("A sync error occurred", e)
                    }
                }

                channel.close()
            })

            scanThread.start()
            val listener = newListener(_ => LOGGER.info("Socket channel closed"))
            channel.closeFuture().addListener(listener).sync()
        } catch {
            case e: InterruptedException =>
            LOGGER.error("A sync error occurred", e)
        } finally {
            workerGroup.shutdownGracefully()
        }
    }
}


class Server(bindAddress: InetSocketAddress) extends Runnable {

    private final val ACCEPTOR_THREADS = 1
    private final val WORKER_THREADS = 1
    private final val LOGGER = LoggerFactory.getLogger(classOf[Server])

    override
    def run() = {
        LOGGER.info("Starting server on [{}]", bindAddress)
        val acceptorGroup = new UcxEventLoopGroup(ACCEPTOR_THREADS)
        val workerGroup = new UcxEventLoopGroup(WORKER_THREADS)
        val bootstrap = new ServerBootstrap()

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(classOf[UcxServerSocketChannel])
            .childHandler(
                new ChannelInitializer[SocketChannel]() {
                    override
                    protected def initChannel(channel: SocketChannel) = {
                        val listener = newListener(_ => LOGGER.info("Socket channel closed"))
                        channel.closeFuture().addListener(listener)
                        channel.pipeline().addLast("echo", new Handler())
                    }
                })
        val eventLoop = acceptorGroup.next();

        val task = new Runnable() { override def run() = { } }
        val f1 = eventLoop.schedule(task, Long.MaxValue, java.util.concurrent.TimeUnit.MILLISECONDS);
        assert(f1.awaitUninterruptibly(1000) == false);
        assert(f1.cancel(true));

        val f2Start = System.currentTimeMillis()
        eventLoop.schedule(task, 1000, java.util.concurrent.TimeUnit.MILLISECONDS).get()
        assert(System.currentTimeMillis() - f2Start >= 1000)
        LOGGER.info("schedule success")

        try {
            val listener = newListener(_ => LOGGER.info("Server socket channel closed"))
            bootstrap.bind(bindAddress).addListener(new GenericFutureListener[ChannelFuture] {
                override
                def operationComplete(future: ChannelFuture) = {
                    if (future.isSuccess()) {
                        LOGGER.info("Server is running")
                    } else {
                        LOGGER.error("Unable to start server", future.cause())
                    }
                }
            }).channel().closeFuture().addListener(listener).sync()
        } catch {
            case e: InterruptedException =>
            LOGGER.error("A sync error occurred", e)
        } finally {
            workerGroup.shutdownGracefully()
            acceptorGroup.shutdownGracefully()
        }
    }
}

class Handler extends ChannelInboundHandlerAdapter {

    private final val LOGGER = LoggerFactory.getLogger(classOf[Handler])

    override
    def channelActive(context: ChannelHandlerContext) = {
        if (context.channel().parent() != null) {
            LOGGER.info("Accepted incoming connection from [{}]", context.channel().remoteAddress())
        } else {
            LOGGER.info("Successfully connected to [{}]", context.channel().remoteAddress())
        }
    }

    override
    def channelReadComplete(context: ChannelHandlerContext) = {
        context.flush();
    }

    override
    def channelRead(context: ChannelHandlerContext, message: Object) = {
        val buffer = message.asInstanceOf[ByteBuf]
        val bytes = new Array[Byte](buffer.readableBytes())

        for (i <- 0 until bytes.length) {
            bytes(i) = buffer.readByte()
        }

        buffer.release()
        val string = new String(bytes, StandardCharsets.UTF_8)
        LOGGER.info("Received message: [{}]", string)

        if (context.channel().parent() != null) {
            val listener = newListener(_ => LOGGER.info("Echo reply sent"))
            context.write(Unpooled.copiedBuffer(string, StandardCharsets.UTF_8)).addListener(listener)
        }
    }

    override
    def exceptionCaught(context: ChannelHandlerContext, cause: Throwable) = {
        LOGGER.error("An exception occurred", cause)
        context.channel().close()
    }
}

case class newListener(onComplete: (ChannelFuture) => Unit)
    extends GenericFutureListener[ChannelFuture] {
    override
    def operationComplete(future: ChannelFuture) = onComplete(future)
}