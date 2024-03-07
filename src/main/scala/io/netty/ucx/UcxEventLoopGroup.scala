package io.netty.channel.ucx

import io.netty.channel.DefaultSelectStrategyFactory
import io.netty.channel.EventLoop
import io.netty.channel.EventLoopGroup
import io.netty.channel.EventLoopTaskQueueFactory
import io.netty.channel.MultithreadEventLoopGroup
import io.netty.channel.SelectStrategyFactory
import io.netty.util.concurrent.EventExecutorChooserFactory
import io.netty.util.concurrent.RejectedExecutionHandler
import io.netty.util.concurrent.RejectedExecutionHandlers
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory
import io.netty.channel.epoll.Epoll

import java.util.concurrent.Executor
import java.util.concurrent.ThreadFactory

import org.openucx.jucx.{NativeLibs, UcxException}
import org.openucx.jucx.ucp._
/**
 * {@link EventLoopGroup} which uses epoll under the covers. Because of this
 * it only works on linux.
 */
class UcxEventLoopGroup(
    nThreads: Int = 0, executor: Executor = null,
    chooserFactory: EventExecutorChooserFactory = DefaultEventExecutorChooserFactory.INSTANCE,
    selectStrategyFactory: SelectStrategyFactory = DefaultSelectStrategyFactory.INSTANCE,
    rejectedExecutionHandler: RejectedExecutionHandler = RejectedExecutionHandlers.reject(),
    queueFactory: EventLoopTaskQueueFactory = null,
    private[ucx] val ucpContext: UcpContext = new UcpContext(UcxEventLoopGroup.ucpParams))
    extends MultithreadEventLoopGroup(
        nThreads, executor, chooserFactory, 0.asInstanceOf[Object], selectStrategyFactory,
        rejectedExecutionHandler, queueFactory) {

    def this(nThreads: Int, executor: Executor,
             selectFactory: SelectStrategyFactory ) {
        this(nThreads, executor, selectStrategyFactory = selectFactory)
    }

    override
    protected def newChild(executor: Executor, args: Object*): EventLoop = {
        return new UcxEventLoop(this, executor, args(0).asInstanceOf[Int],
                args(1).asInstanceOf[SelectStrategyFactory].newSelectStrategy(),
                args(2).asInstanceOf[RejectedExecutionHandler],
                args(3).asInstanceOf[EventLoopTaskQueueFactory])
    }
}

object UcxEventLoopGroup {
    final val loadNative = {
        Epoll.ensureAvailability()
        NativeLibs.load()
        true
    }

    final val ucpParams = new UcpParams().requestAmFeature().requestWakeupFeature()
            .setMtWorkersShared(true).setConfig("USE_MT_MUTEX", "yes")
            .setEstimatedNumEps(4000) // TODO: estimate eps
}