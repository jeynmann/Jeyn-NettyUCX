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

    final val FILE_REGION = 0
    final val BYTE_BUF = 1

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
    private var msgType = PerfDemo.FILE_REGION
    private var numWorker = PerfDemo.DEFAULT_THREADS
    private var numIter = 1024
    private var bufSize = 2 << 20
    private var frameSize = 32 << 10
    private var filePrefix = "/images/hadoop_log/tmp"

    override
    def run(): Unit = {
        transport = PerfDemo.getEnv("transport", s"$transport").toInt
        msgType = PerfDemo.getEnv("msgType", s"$msgType").toInt
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
        val totalSize = bufSize.toLong * numIter
        val latch = new java.util.concurrent.CountDownLatch(numWorker)

        bootstrap.group(workerGroup)
            .channel(channelClazz)
            .handler(new ChannelInitializer[SocketChannel]() {
                override
                protected def initChannel(channel: SocketChannel) = {
                    val handler = new PerfHandler(totalSize, false)
                    val listener = newListener(_ => {
                        logInfo("Socket channel closed")
                        latch.countDown()
                    })
                    channel.closeFuture().addListener(listener)
                    channel.pipeline().addLast("handler", handler)
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

                    val length = file.length
                    var send = 0l
                    var offset = 0l
                    for (i <- 0 until numIter) {
                        val msg = msgType match {
                            case PerfDemo.FILE_REGION =>
                                new DefaultFileRegion(file, offset, bufSize)
                            case _ => {
                                val m = channel.alloc.heapBuffer(bufSize.toInt)
                                m.writerIndex(m.capacity())
                                m
                            }
                        }
                        channel.writeAndFlush(msg)

                        if ((i == 0) || (i == (numIter - 1))) {
                            logDebug(s"<$file> [input] {$i}:{$msg}")
                        }

                        send += bufSize
                        offset += bufSize
                        if (offset + bufSize > length) {
                            offset = 0
                        }
                    }

                    latch.await()
                    val costTime = System.nanoTime() - startTime
                    wrokerCost(id) = costTime / 1000000.0
                    wrokerSend(id) = send / 1048576.0
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
    private var msgType = PerfDemo.FILE_REGION
    private var numAcceptor = PerfDemo.DEFAULT_THREADS
    private var numWorker = PerfDemo.DEFAULT_THREADS
    private var numIter = 1024
    private var bufSize = 2 << 20

    override
    def run() = {
        transport = PerfDemo.getEnv("transport", s"$transport").toInt
        msgType = PerfDemo.getEnv("msgType", s"$msgType").toInt
        numAcceptor = PerfDemo.getEnv("numAcceptor", s"$numAcceptor").toInt
        numWorker = PerfDemo.getEnv("numWorker", s"$numWorker").toInt
        numIter = PerfDemo.getEnv("numIter", s"$numIter").toInt
        bufSize = PerfDemo.getEnv("bufSize", s"$bufSize").toInt

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
        val totalSize = bufSize.toLong * numIter

        bootstrap.group(acceptorGroup, workerGroup)
            .channel(channelClazz)
            .childHandler(new ChannelInitializer[SocketChannel]() {
                override
                protected def initChannel(channel: SocketChannel) = {
                    val handler = new PerfHandler(totalSize, true)
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

class PerfHandler(targetSize: Long, isServer: Boolean)
    extends ChannelInboundHandlerAdapter with UcxLogging {

    private var unsafeTime = 0l
    private var unsafeSize = 0l
    private var unsafeIter = 0l

    override
    def channelActive(context: ChannelHandlerContext) = {
        if (isServer) {
            logInfo(s"Accepted incoming connection from [${context.channel().remoteAddress()}]")
        } else {
            logInfo(s"Successfully connected to [${context.channel().remoteAddress()}]")
        }
        unsafeTime = System.nanoTime()
    }

    override
    def channelRead(context: ChannelHandlerContext, msg: Object) = {
        val channel = context.channel()
        val message = msg.asInstanceOf[ByteBuf]
        val readableBytes = message.readableBytes()

        unsafeIter += 1

        if (isServer) {
            val ack = channel.alloc().directBuffer(4)
            ack.writeInt(readableBytes)
            channel.writeAndFlush(ack)

            unsafeSize += readableBytes
            if ((unsafeSize == readableBytes) || (unsafeSize >= targetSize)) {
                val str = message.toString(0, readableBytes.min(16),
                                           java.nio.charset.StandardCharsets.UTF_8)
                logDebug(s"[receive] {$unsafeIter}:{$msg}:{$str}")
            }
        } else {
            val ackSize = message.readInt()
            unsafeSize += ackSize
        }

        message.release()

        if (unsafeSize >= targetSize) {
            val timeNow = System.nanoTime()
            val cost = (timeNow - unsafeTime) / 1000000000.0
            val size = unsafeSize / 1048576.0
            logDebug(s"[Fin] {$unsafeIter} [Len] {$size} [BW] {${size/cost}}MB/s")
            channel.eventLoop().execute(new Runnable {
                override def run = channel.close()
            })
        }
    }

    override
    def exceptionCaught(context: ChannelHandlerContext, cause: Throwable) = {
        logError("An exception occurred", cause)
        context.channel().close()
    }
}
