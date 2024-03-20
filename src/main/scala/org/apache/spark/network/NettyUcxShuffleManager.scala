/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import java.util.concurrent.{TimeUnit, Callable}
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.rpc.{RpcEnv, RpcEndpointRef}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.network.netty.NettyUcxBlockTransferService
import org.openucx.jucx.{NativeLibs, UcxException}

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

  private[spark] val ucxThread = ThreadUtils.newDaemonSingleThreadExecutor("UCX-setup")
  private[spark] val ucxCores = conf.getInt("spark.executor.cores", 2)
  private[spark] val latch = ucxThread.submit(new Callable[NettyUcxBlockTransferService] {
    override def call = {
      while (SparkEnv.get == null) {
        Thread.sleep(10)
      }

      val env = SparkEnv.get
      while (env.blockManager.blockManagerId == null) {
        Thread.sleep(10)
      }

      val blockManagerId = env.blockManager.blockManagerId
      val shuffleService = new NettyUcxBlockTransferService(
        conf,
        env.securityManager,
        blockManagerId.host,
        blockManagerId.host,
        blockManagerId.port + 1 /* TODO */,
        ucxCores)

      shuffleService.init(env.blockManager)
      blockTransferService.set(shuffleService)
      shuffleService
    }
  })

  private[spark] val blockTransferService = new AtomicReference[NettyUcxBlockTransferService]

  def shuffleClient(): NettyUcxBlockTransferService = latch.get(10, TimeUnit.SECONDS)

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

}