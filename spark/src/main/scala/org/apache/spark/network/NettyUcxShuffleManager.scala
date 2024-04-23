/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.util.concurrent.{TimeUnit, Callable}
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

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

case class ExecutorAdded(endpoint: RpcEndpointRef, execId: Int, host: String, port: Int)
case class IntroduceAllExecutors(execAddress: Map[Int, (String, Int)])

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints = mutable.HashSet.empty[RpcEndpointRef]
  private val execAddress = mutable.HashMap.empty[Int, (String, Int)]

  override def receive: PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(endpoint: RpcEndpointRef, execId: Int, host: String,
                               port: Int) => {
      // Driver receives a message from executor with it's workerAddress
      // 2. For each existing member introduce newly joined executor.
      logDebug(s"send ${endpoints.size} $message")
      endpoints.foreach(_.send(message))
      endpoints += endpoint
      // 1. Introduce existing members of a cluster
      logDebug(s"receive $message")
      if (execAddress.nonEmpty) {
        endpoint.send(IntroduceAllExecutors(execAddress.toMap))
      }
      execAddress += execId -> (host, port)
    }
  }
}

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv,
                             shuffleManager: NettyUcxShuffleManager)
  extends RpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(_: RpcEndpointRef, execId: Int, host: String, port: Int) => {
      shuffleManager.addServer(execId, (host, port))
      shuffleManager.connectNext()
    }
    case IntroduceAllExecutors(execAddress: Map[Int, (String, Int)]) => {
      shuffleManager.addServers(execAddress)
      shuffleManager.connectNext()
    }
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
  private[spark] val execAddress = new scala.collection.concurrent.TrieMap[String, (String, Int)]
  private[spark] val connectQueue = new java.util.concurrent.ConcurrentLinkedQueue[String]
  private[spark] val ucxThread = ThreadUtils.newDaemonSingleThreadExecutor("UCX-setup")
  private[spark] val ucxCores = conf.getInt("spark.executor.cores", 2)

  private var executorEndpoint: UcxExecutorRpcEndpoint = _
  private var driverEndpoint: UcxDriverRpcEndpoint = _
  private val connecting = new AtomicBoolean(false)

  private val connectTask: Runnable = new Runnable {
    override def run(): Unit = {
      if (!connectQueue.isEmpty()) {
        val execId = connectQueue.poll()
        execAddress.get(execId).foreach(addr =>
          shuffleClient.connectUcxService(addr._1, addr._2))
      }

      if (!connectQueue.isEmpty() || !connecting.compareAndSet(true, false)) {
        ucxThread.submit(connectTask)
      }
    }
  }

  private[spark] val latch = ucxThread.submit(new Callable[NettyUcxBlockTransferService] {
    override def call(): NettyUcxBlockTransferService = {
      sleepUntil(() => {
        SparkEnv.get != null
      }, 10000, 10, "get SparkEnv timeout")

      val env = SparkEnv.get
      val rpcEnv = env.rpcEnv
      val ucxDriver = "UCX-driver"
      if (isDriver) {
        driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(ucxDriver, driverEndpoint)
        logInfo(s"start ucx driver: success.")
        return null
      }

      sleepUntil(() => {
        env.blockManager.blockManagerId != null
      }, 10000, 10, "get blockManagerId timeout")

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
      }, 10000, 10, s"get ucxDriver($rpcEnv) timeout")

      driverEndpointRef.send(
        ExecutorAdded(endpoint, blockManagerId.executorId.toInt, blockManagerId.host,
                      shuffleService.port))

      logInfo(s"start shuffle service($ucxCores): success.")

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

  def getUcxPort(executorId: String): Int = {
    execAddress.getOrElse(executorId, {
      sleepUntil(() => execAddress.contains(executorId), 10000, 10,
                 s"get exec $executorId timeout")
      execAddress(executorId)
    })._2
  }

  def addServer(execId: Int, addr: (String, Int)): Unit = {
    val executorId = execId.toString
    execAddress += executorId -> addr
    connectQueue.add(executorId)
  }

  def addServers(execMap: Map[Int, (String, Int)]): Unit = {
    execMap.foreach(execAddr => addServer(execAddr._1, execAddr._2))
  }

  def connectNext(): Unit = {
    if (!connectQueue.isEmpty() && connecting.compareAndSet(false, true)) {
      ucxThread.submit(connectTask)
    }
  }

  def sleepUntil(condition: () => Boolean, timeoutMs: Long, sleepMs: Long, err: => String) {
    if (!condition()) {
      val deadlineNs = System.nanoTime + timeoutMs * 1000000L
      do {
        Thread.sleep(sleepMs)
        if (System.nanoTime > deadlineNs) {
          logError(err)
          throw new UcxException(err)
        }
      } while (!condition())
    }
  }
}