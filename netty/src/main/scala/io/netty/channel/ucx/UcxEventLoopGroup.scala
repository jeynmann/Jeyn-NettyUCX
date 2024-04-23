package io.netty.channel.ucx

import io.netty.buffer.UcxPooledByteBufAllocator
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

import org.openucx.jucx.ucp._
/**
 * {@link EventLoopGroup} which uses epoll under the covers. Because of this
 * it only works on linux.
 */
class UcxEventLoopGroup(nThreads: Int = 0, executor: Executor = null,
    chooserFactory: EventExecutorChooserFactory = DefaultEventExecutorChooserFactory.INSTANCE,
    selectStrategyFactory: SelectStrategyFactory = DefaultSelectStrategyFactory.INSTANCE,
    rejectedExecutionHandler: RejectedExecutionHandler = RejectedExecutionHandlers.reject(),
    queueFactory: EventLoopTaskQueueFactory = null,
    val ucpContext: UcpContext = UcxPooledByteBufAllocator.UCP_CONTEXT)
    extends MultithreadEventLoopGroup(nThreads, executor, chooserFactory,
        selectStrategyFactory, rejectedExecutionHandler, queueFactory, ucpContext) with UcxLogging {
    logDev(s"UcxEventLoopGroup() nThreads $nThreads executor $executor chooserFactory $chooserFactory")

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
        return new UcxEventLoop(this, executor,
                args(0).asInstanceOf[SelectStrategyFactory].newSelectStrategy(),
                args(1).asInstanceOf[RejectedExecutionHandler],
                args(2).asInstanceOf[EventLoopTaskQueueFactory],
                args(3).asInstanceOf[UcpContext])
    }
}
