/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.util.concurrent.{TimeUnit, Callable}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark._
import org.apache.spark.rpc._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.network.netty.NettyUcxBlockTransferService

import org.openucx.jucx.{NativeLibs, UcxException}

case class ExecutorAdded(endpoint: RpcEndpointRef, host: String, port: Int, ucxPort: Int)
case class IntroduceAllExecutors(serviceMap: Map[(String, Int), Int])

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints = mutable.HashSet.empty[RpcEndpointRef]
  private val serviceMap = mutable.HashMap.empty[(String, Int), Int]

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(endpoint: RpcEndpointRef, host: String, port: Int,
                               ucxPort: Int) => {
      // Driver receives a message from executor with it's workerAddress
      // 1. Introduce existing members of a cluster
      logDebug(s"receive $message")
      if (serviceMap.nonEmpty) {
        context.reply(IntroduceAllExecutors(serviceMap.toMap))
      }
      serviceMap += (host, port) -> ucxPort
      // 2. For each existing member introduce newly joined executor.
      logDebug(s"send $endpoints $message")
      endpoints.foreach(_.send(message))
      endpoints += endpoint
    }
  }
}

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv,
                             shuffleManager: NettyUcxShuffleManager)
  extends RpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(_: RpcEndpointRef, host: String, port: Int, ucxPort: Int) =>
      shuffleManager.ucxThread.submit(new Runnable {
        override def run = shuffleManager.addServer((host, port), ucxPort)
      })
    case IntroduceAllExecutors(serviceMap: Map[(String, Int), Int]) =>
      shuffleManager.ucxThread.submit(new Runnable {
        override def run = shuffleManager.addServers(serviceMap)
      })
  }
}

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
class NettyUcxShuffleManager(val conf: SparkConf, isDriver: Boolean)
  extends SortShuffleManager(conf) with Logging {

  if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
    throw new UnsupportedOperationException("externl not support")
  }

  if (!isDriver) {
    /* Load UCX/JUCX libraries as soon as possible to avoid collision with JVM when register malloc/mmap hook. */
    NativeLibs.load();
  }

  private[spark] val blockTransferService = new AtomicReference[NettyUcxBlockTransferService]
  private[spark] val serviceMap = new scala.collection.concurrent.TrieMap[(String, Int), Int]
  private[spark] val ucxDriver = "UCX-driver"
  private[spark] val ucxThread = ThreadUtils.newDaemonSingleThreadExecutor("UCX-setup")
  private[spark] val ucxCores = conf.getInt("spark.executor.cores", 2)

  private var executorEndpoint: UcxExecutorRpcEndpoint = _
  private var driverEndpoint: UcxDriverRpcEndpoint = _

  private[spark] val latch = ucxThread.submit(new Callable[NettyUcxBlockTransferService] {
    override def call(): NettyUcxBlockTransferService = {
      sleepUntil(() => {
        SparkEnv.get != null
      }, 10000, 10, "get SparkEnv timeout")

      val env = SparkEnv.get
      sleepUntil(() => {
        env.blockManager.blockManagerId != null
      }, 10000, 10, "get blockManagerId timeout")

      val rpcEnv = SparkEnv.get.rpcEnv
      if (isDriver) {
        driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(ucxDriver, driverEndpoint)
        logInfo(s"start ucx driver: success.")
        return null
      }

      val blockManagerId = env.blockManager.blockManagerId
      val shuffleService = new NettyUcxBlockTransferService(
        conf,
        NettyUcxShuffleManager.this,
        env.securityManager,
        blockManagerId.host,
        blockManagerId.host,
        0,
        ucxCores)

      shuffleService.init(env.blockManager)

      executorEndpoint = new UcxExecutorRpcEndpoint(rpcEnv, NettyUcxShuffleManager.this)
      val endpoint = rpcEnv.setupEndpoint(s"UCX-${blockManagerId.executorId}",
                                          executorEndpoint)
      var driverEndpointRef: RpcEndpointRef = null
      sleepUntil(() => {
        try {
          driverEndpointRef = RpcUtils.makeDriverRef(ucxDriver, conf, rpcEnv)
        } catch {
          case _: SparkException => {}
        }
        driverEndpointRef != null
      }, 10000, 10, "get ucxDriver timeout")

      driverEndpointRef.ask[IntroduceAllExecutors](
        ExecutorAdded(endpoint, blockManagerId.host, blockManagerId.port,
                      shuffleService.port))
        .andThen {
          case Success(msg) =>
            logDebug(s"receive ${msg.asInstanceOf[IntroduceAllExecutors].serviceMap.keys}")
            executorEndpoint.receive(msg)
        }

      logInfo(s"start shuffle service: success.")

      blockTransferService.set(shuffleService)
      shuffleService
    }
  })

  def shuffleClient(): NettyUcxBlockTransferService = {
    val service = blockTransferService.get()
    if (service != null) {
      return service
    }
    return latch.get(10, TimeUnit.SECONDS)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new NettyUcxBlockStoreShuffleReader(
      this, handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  def getUcxPort(tcpServer: (String, Int)): Int = {
    serviceMap.getOrElse(tcpServer, {
      sleepUntil(() => serviceMap.contains(tcpServer), 10000, 10, s"get $tcpServer timeout")
      serviceMap(tcpServer)
    })
  }

  def addServer(tcpServer: (String, Int), ucxPort: Int): Unit = {
    serviceMap += tcpServer -> ucxPort
    shuffleClient.connectUcxService(tcpServer._1, ucxPort)
  }

  def addServers(serviceMap: Map[(String, Int), Int]): Unit = {
    serviceMap.foreach(servicePort => addServer(servicePort._1, servicePort._2))
  }

  def sleepUntil(condition: () => Boolean, timeoutMs: Long, sleepMs: Long, err: => String) {
    if (!condition()) {
      val deadlineNs = System.nanoTime + timeoutMs * 1000000L
      do {
        Thread.sleep(sleepMs)
        if (System.nanoTime > deadlineNs) {
          throw new UcxException(err)
        }
      } while (!condition())
    }
  }
}