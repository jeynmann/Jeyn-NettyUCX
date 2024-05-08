package io.netty.channel.ucx.examples

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.io.File
import java.util.Scanner

import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator

import io.netty.buffer.ByteBuf
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap

import io.netty.channel.ucx._
import io.netty.channel.ChannelOption
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.DefaultFileRegion
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.GenericFutureListener

class PerfDemo(
    isServer: Boolean,
    bindAddress: InetSocketAddress, remoteAddress: InetSocketAddress)
    extends Runnable with UcxLogging {

    def this() = {
        this(true, new InetSocketAddress(PerfDemo.DEFAULT_SERVER_PORT), null)
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
            logError("Please specify the server address")
            return
        }

        UcxPooledByteBufAllocator.loadNativeLibs()
        val runnable = if (isServer)
            new PerfServer(bindAddress)
        else
            new PerfClient(bindAddress, remoteAddress)
        runnable.run()
    }
}

object PerfDemo {
    final val DEFAULT_SERVER_PORT = 2998
    final val DEFAULT_THREADS = 1

    final val NIO_TRANSPORT = 0
    final val UCX_TRANSPORT = 1

    def main(args: Array[String]): Unit = {
        initializeLogging()
        PerfDemo(args).run()
    }

    def apply(args: Array[String]): PerfDemo = {  
        args.length match {
            case 0 => new PerfDemo()
            case 1 => new PerfDemo(new InetSocketAddress(args(0).toInt)) // port <-
            case 2 => new PerfDemo(null, new InetSocketAddress(args(0), args(1).toInt)) // -> host port
            case 3 => new PerfDemo(new InetSocketAddress(args(0).toInt), new InetSocketAddress(args(1), args(2).toInt)) // port -> host port
            case _ => new PerfDemo(new InetSocketAddress(args(0), args(1).toInt), new InetSocketAddress(args(2), args(3).toInt)) // host port -> host port
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

    def getEnv(k: String, v: String = ""): String = {
        val e = System.getenv(k)
        if (e != null) {
            return e
        } else {
            return v
        }
    }
}

class PerfClient(bindAddress: InetSocketAddress, remoteAddress: InetSocketAddress)
    extends Runnable with UcxLogging {

    private final val scanner: Scanner  = new Scanner(System.in)
    private var transport = PerfDemo.UCX_TRANSPORT
    private var numWorker = PerfDemo.DEFAULT_THREADS
    private var numIter = 1024
    private var bufSize = 2 << 20
    private var frameSize = 32 << 10
    private var filePrefix = "/images/hadoop_log/tmp"

    override
    def run(): Unit = {
        transport = PerfDemo.getEnv("transport", s"$transport").toInt
        numWorker = PerfDemo.getEnv("numWorker", s"$numWorker").toInt
        numIter = PerfDemo.getEnv("numIter", s"$numIter").toInt
        bufSize = PerfDemo.getEnv("bufSize", s"$bufSize").toInt
        frameSize = PerfDemo.getEnv("frameSize", s"$frameSize").toInt
        filePrefix = PerfDemo.getEnv("filePrefix", s"$filePrefix")

        logInfo(s"Connecting to server [$remoteAddress]")
        val bootstrap = new Bootstrap()
        val workerGroup = transport match {
            case PerfDemo.NIO_TRANSPORT => new NioEventLoopGroup(numWorker)
            case _ => new UcxEventLoopGroup(numWorker)
        }
        val channelClazz = transport match {
            case PerfDemo.NIO_TRANSPORT => classOf[NioSocketChannel]
            case _ => classOf[UcxSocketChannel]
        }

        bootstrap.group(workerGroup)
            .channel(channelClazz)
            .handler(new ChannelInitializer[SocketChannel]() {
                override
                protected def initChannel(channel: SocketChannel) = {
                    val listener = newListener(_ => logInfo("Socket channel closed"))
                    channel.closeFuture().addListener(listener)
                    channel.pipeline().addLast("handler", new PerfHandler())
                }
            })
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS.asInstanceOf[ChannelOption[Any]], 5000)


        val wrokerCost = new Array[Double](numWorker)
        val wrokerSend = new Array[Double](numWorker)
        val threads = (0 until numWorker).map(id => {
            new Thread(() => {
                try {
                    val startTime = System.nanoTime()
                    val file = new File(s"$filePrefix$id.txt")
                    val channel = bootstrap.connect(remoteAddress, bindAddress).sync().channel()
                    channel match {
                        case ucx: UcxSocketChannel =>
                            ucx.config().setFileFrameSize(frameSize)
                        case _: SocketChannel => {}
                    }

                    val debugMask = (4096 - 1)
                    val length = file.length
                    var send = 0l
                    var offset = 0l
                    var inFlight = numIter
                    for (i <- 0 until numIter) {
                        val msg = new DefaultFileRegion(file, offset, bufSize)
                        val listener = newListener(_ => inFlight -= 1)
                        channel.writeAndFlush(msg).addListener(listener)

                        if ((i & debugMask) == 0) {
                            logDebug(s"<$file> [input] {$i}:{$msg}")
                        }

                        send += bufSize
                        offset += bufSize
                        if (offset + bufSize > length) {
                            offset = 0
                        }
                    }

                    while (inFlight != 0) {
                        Thread.`yield`()
                    }
                    val costTime = System.nanoTime() - startTime
                    wrokerCost(id) = costTime / 1000000.0
                    wrokerSend(id) = send / 1048576.0
                    channel.close().sync()
                } catch {
                    case e: InterruptedException =>
                    logError("A sync error occurred", e)
                } finally {
                    workerGroup.shutdownGracefully()
                }
            })
        })
        threads.foreach(_.start)
        threads.foreach(_.join)
        logInfo(s"cost ${wrokerCost.sum / wrokerCost.size} ms " +
                s"bw ${wrokerCost.sum * 1000.0 / wrokerSend.sum} MB/s")
    }
}


class PerfServer(bindAddress: InetSocketAddress) extends Runnable with UcxLogging {

    private var transport = PerfDemo.UCX_TRANSPORT
    private var numAcceptor = PerfDemo.DEFAULT_THREADS
    private var numWorker = PerfDemo.DEFAULT_THREADS

    override
    def run() = {
        transport = PerfDemo.getEnv("transport", s"$transport").toInt
        numAcceptor = PerfDemo.getEnv("numAcceptor", s"$numAcceptor").toInt
        numWorker = PerfDemo.getEnv("numWorker", s"$numWorker").toInt

        logInfo(s"Starting server on [$bindAddress]")
        val bootstrap = new ServerBootstrap()
        val acceptorGroup = transport match {
            case PerfDemo.NIO_TRANSPORT => new NioEventLoopGroup(numAcceptor)
            case _ => new UcxEventLoopGroup(numAcceptor)
        }
        val workerGroup = transport match {
            case PerfDemo.NIO_TRANSPORT => new NioEventLoopGroup(numWorker)
            case _ => new UcxEventLoopGroup(numWorker)
        }
        val channelClazz = transport match {
            case PerfDemo.NIO_TRANSPORT => classOf[NioServerSocketChannel]
            case _ => classOf[UcxServerSocketChannel]
        }

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(channelClazz)
            .childHandler(new ChannelInitializer[SocketChannel]() {
                override
                protected def initChannel(channel: SocketChannel) = {
                    val handler = transport match {
                        case PerfDemo.NIO_TRANSPORT => new PerfHandler(2)
                        case _ => new PerfHandler()
                    }

                    val listener = newListener(_ => logInfo("Socket channel closed"))
                    channel.closeFuture().addListener(listener)
                    channel.pipeline().addLast("handler", handler)
                }
            })

        try {
            val listener = newListener(_ => logInfo("PerfServer socket channel closed"))
            bootstrap.bind(bindAddress).addListener(new GenericFutureListener[ChannelFuture] {
                override
                def operationComplete(future: ChannelFuture) = {
                    if (future.isSuccess()) {
                        logInfo("PerfServer is running")
                    } else {
                        logError("Unable to start server", future.cause())
                    }
                }
            }).channel().closeFuture().addListener(listener).sync()
        } catch {
            case e: InterruptedException =>
            logError("A sync error occurred", e)
        } finally {
            workerGroup.shutdownGracefully()
            acceptorGroup.shutdownGracefully()
        }
    }
}

class PerfHandler(shift: Int = 0) extends ChannelInboundHandlerAdapter with UcxLogging {

    private var i = new java.util.concurrent.atomic.AtomicInteger()
    private val debugMask = ((4096 << shift) - 1)

    override
    def channelActive(context: ChannelHandlerContext) = {
        if (context.channel().parent() != null) {
            logInfo(s"Accepted incoming connection from [${context.channel().remoteAddress()}]")
        } else {
            logInfo(s"Successfully connected to [${context.channel().remoteAddress()}]")
        }
    }

    override
    def channelRead(context: ChannelHandlerContext, msg: Object) = {
        val message = msg.asInstanceOf[ByteBuf]
        val readableBytes = message.readableBytes()

        if ((i.incrementAndGet() & debugMask) == 0) {
            val str = message.toString(0, readableBytes.min(16),
                                          java.nio.charset.StandardCharsets.UTF_8)
            logDebug(s"[receive] {$i}:{$msg}:{$str}")
        }

        message.release()
    }

    override
    def exceptionCaught(context: ChannelHandlerContext, cause: Throwable) = {
        logError("An exception occurred", cause)
        // context.channel().close()
    }
}
