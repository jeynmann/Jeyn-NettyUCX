/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.client

import java.io.Closeable
import java.io.IOException
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.List
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricSet
import com.google.common.base.Preconditions
import com.google.common.base.Throwables
import com.google.common.collect.Lists
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.ucx._
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.network.TransportContext
import org.apache.spark.network.protocol.NettyUcxMessageEncoder
import org.apache.spark.network.netty.NettyUcxBlockTransferService
import org.apache.spark.network.server.TransportChannelHandler
import org.apache.spark.network.util._

/** A simple data structure to track the pool of clients between two peer nodes. */
class UcxClientPool(size: Int) {
  val clients = new Array[TransportClient](size)
  val locks = new Array[Object](size)

  (0 until size).foreach(locks(_) = new Object())
}

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
class NettyUcxTransportClientFactory(
  context: TransportContext,
  conf: TransportConf,
  clientBootstraps: List[TransportClientBootstrap],
  connectionPool: ConcurrentHashMap[SocketAddress, UcxClientPool],
  numConnectionsPerPeer: Int,
  rand: Random) extends Closeable {

  private val logger = LoggerFactory.getLogger(classOf[NettyUcxTransportClientFactory])
  private val fileFrameSize = conf.getInt(
    NettyUcxBlockTransferService.FILE_FRAME_SIZE_KEY,
    NettyUcxBlockTransferService.FILE_FRAME_SIZE_DEFAULT)

  private var socketChannelClass = classOf[UcxSocketChannel]
  private var workerGroup: EventLoopGroup = _
  private var pooledAllocator: UcxPooledByteBufAllocator = _
  private var metrics: NettyMemoryMetrics = _

  def this(
      context: TransportContext,
      clientBootstraps: List[TransportClientBootstrap]) = {
    this(
      Preconditions.checkNotNull(context),
      context.getConf(),
      Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps)),
      new ConcurrentHashMap[SocketAddress, UcxClientPool](),
      context.getConf().numConnectionsPerPeer(),
      new Random())

    val clientPoolPrefix = "UCX-shuffle-client"
    val clientThreadFactory = new DefaultThreadFactory(clientPoolPrefix, true)
    val numCores = conf.clientThreads()
    workerGroup = new UcxEventLoopGroup(numCores, clientThreadFactory)
    pooledAllocator = new UcxPooledByteBufAllocator(
      Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
      Math.min(PooledByteBufAllocator.defaultNumDirectArena(), numCores),
      PooledByteBufAllocator.defaultPageSize(),
      PooledByteBufAllocator.defaultMaxOrder(),
      0, 0, 0, false
    )
    metrics = new NettyMemoryMetrics(pooledAllocator, clientPoolPrefix, conf)
    logger.info("create factory with fileFrameSize {}", fileFrameSize)
  }

  def getAllMetrics(): MetricSet = metrics

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   */
  def createClient(remoteHost: String, remotePort: Int): TransportClient = {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    val unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort)

    // Create the UcxClientPool if we don't have it yet.
    var clientPool = connectionPool.get(unresolvedAddress)
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new UcxClientPool(numConnectionsPerPeer))
      clientPool = connectionPool.get(unresolvedAddress)
    }

    val clientIndex = rand.nextInt(numConnectionsPerPeer)
    var cachedClient = clientPool.clients(clientIndex)

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      val handler = cachedClient.getChannel().pipeline()
        .get(classOf[TransportChannelHandler])
      handler.synchronized {
        handler.getResponseHandler().updateTimeOfLastRequest()
      }

      if (cachedClient.isActive()) {
        logger.trace(s"Returning cached connection to ${cachedClient.getSocketAddress()}: ${cachedClient}")
        return cachedClient
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    val preResolveHost = System.nanoTime()
    val resolvedAddress = new InetSocketAddress(remoteHost, remotePort)
    val hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs)
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs)
    }

    clientPool.locks(clientIndex).synchronized {
      cachedClient = clientPool.clients(clientIndex)

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace(s"Returning cached connection to ${resolvedAddress}: ${cachedClient}")
          return cachedClient
        } else {
          logger.info(s"Found inactive connection to ${resolvedAddress}, creating a new one.")
        }
      }
      clientPool.clients(clientIndex) = createClient(resolvedAddress)
      return clientPool.clients(clientIndex)
    }
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, Int)}, this method is blocking.
   */
  def createUnmanagedClient(remoteHost: String, remotePort: Int): TransportClient = {
    val address = new InetSocketAddress(remoteHost, remotePort)
    return createClient(address)
  }

  /** Create a completely new {@link TransportClient} to the remote address. */
  private def createClient(address: InetSocketAddress): TransportClient = {
    logger.debug("Creating new connection to {}", address)

    val bootstrap = new Bootstrap()
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      // .option(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Any]], true)
      // .option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS.asInstanceOf[ChannelOption[Any]], conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator)
      // .option(ChannelOption.FILE_FRAME_SIZE, fileFrameSize)

    // if (conf.receiveBuf() > 0) {
    //   bootstrap.option(ChannelOption.SO_RCVBUF, conf.receiveBuf())
    // }

    // if (conf.sendBuf() > 0) {
    //   bootstrap.option(ChannelOption.SO_SNDBUF, conf.sendBuf())
    // }

    val clientRef = new AtomicReference[TransportClient]()
    val channelRef = new AtomicReference[SocketChannel]()

    bootstrap.handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel) = {
        val clientHandler = context.initializePipeline(ch)
        // ch.pipeline().addAfter("encoder", "ucx_encoder", NettyUcxMessageEncoder.INSTANCE)
        //   .remove("encoder")
        ch.asInstanceOf[UcxSocketChannel].config().setFileFrameSize(fileFrameSize)
        clientRef.set(clientHandler.getClient())
        channelRef.set(ch)
      }
    })

    // Connect to the remote server
    val preConnect = System.nanoTime()
    val cf = bootstrap.connect(address)
    if (!cf.await(conf.connectionTimeoutMs())) {
      throw new IOException(
        s"Connecting to ${address} timed out (${conf.connectionTimeoutMs()} ms)")
    } else if (cf.cause() != null) {
      throw new IOException(s"Failed to connect to ${address}", cf.cause())
    }

    val client = clientRef.get()
    val channel = channelRef.get()

    assert(client != null, "Channel future completed successfully with null client")

    // Execute any client bootstraps synchronously before marking the Client as successful.
    val preBootstrap = System.nanoTime()
    logger.debug("Connection to {} successful, running bootstraps...", address)
    try {
      for (clientBootstrap <- clientBootstraps.asScala) {
        clientBootstrap.doBootstrap(client, channel)
      }
    } catch {
      case e: Exception => { 
        // catch non-RuntimeExceptions too as bootstrap may be written in Scala
        val bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000
        logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e)
        client.close()
        throw Throwables.propagate(e)
      }
    }
    val postBootstrap = System.nanoTime()

    logger.info(s"Successfully created connection to ${address} after ${(postBootstrap - preConnect) / 1000000} ms (${(postBootstrap - preBootstrap) / 1000000} ms spent in bootstraps)")

    return client
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  override def close() = {
    // Go through all clients and close them if they are active.
    for (clientPool <- connectionPool.values().asScala) {
      (0 until clientPool.clients.length).foreach(i => {
        val client = clientPool.clients(i)
        if (client != null) {
          clientPool.clients(i) = null
          JavaUtils.closeQuietly(client)
        }
      })
    }
    connectionPool.clear()

    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully()
    }
  }
}
