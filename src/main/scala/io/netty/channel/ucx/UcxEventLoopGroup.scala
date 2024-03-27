package io.netty.channel.ucx

import io.netty.channel.DefaultSelectStrategyFactory
import io.netty.channel.EventLoop
import io.netty.channel.EventLoopGroup
import io.netty.channel.EventLoopTaskQueueFactory
import io.netty.channel.MultithreadEventLoopGroup
import io.netty.channel.SelectStrategyFactory
import io.netty.util.concurrent.ThreadPerTaskExecutor
import io.netty.util.concurrent.EventExecutorChooserFactory
import io.netty.util.concurrent.RejectedExecutionHandler
import io.netty.util.concurrent.RejectedExecutionHandlers
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory

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
    queueFactory: EventLoopTaskQueueFactory = null)
    extends MultithreadEventLoopGroup(
        nThreads, executor, chooserFactory, 0.asInstanceOf[Object], selectStrategyFactory,
        rejectedExecutionHandler, queueFactory) with UcxLogging {
    logDev(s"UcxEventLoopGroup() nThreads $nThreads executor $executor chooserFactory $chooserFactory")

    private[ucx] val ucpContext: UcpContext = UcxEventLoopGroup.ucpContext

    def this(nThreads: Int, executor: Executor,
             selectFactory: SelectStrategyFactory) = {
        this(nThreads, executor, selectStrategyFactory = selectFactory)
    }

    def this(nThreads: Int, threadFactory: ThreadFactory) = {
        this(nThreads, new ThreadPerTaskExecutor(threadFactory))
    }

    def this(nThreads: Int, threadFactory: ThreadFactory,
             selectFactory: SelectStrategyFactory) = {
        this(nThreads, new ThreadPerTaskExecutor(threadFactory), selectFactory)
    }

    override
    protected def newChild(executor: Executor, args: Object*): EventLoop = {
        logDev(s"newChild() executor $executor args $args")
        return new UcxEventLoop(this, executor, args(0).asInstanceOf[Int],
                args(1).asInstanceOf[SelectStrategyFactory].newSelectStrategy(),
                args(2).asInstanceOf[RejectedExecutionHandler],
                args(3).asInstanceOf[EventLoopTaskQueueFactory])
    }
}

object UcxEventLoopGroup {
    final val loadNative = {
        NativeLibs.load()
        true
    }

    final val ucpParams = new UcpParams().requestAmFeature().requestWakeupFeature()
            .setMtWorkersShared(true).setConfig("USE_MT_MUTEX", "yes")
            .setEstimatedNumEps(4000) // TODO: estimate eps

    final val ucpContext = new UcpContext(UcxEventLoopGroup.ucpParams)
}