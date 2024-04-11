package io.netty.channel.ucx

import java.io.IOException
import java.nio.ByteBuffer
import java.net.InetSocketAddress
import java.util.Queue
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap

import io.netty.channel.EventLoop
import io.netty.channel.EventLoopGroup
import io.netty.channel.EventLoopTaskQueueFactory
import io.netty.channel.SelectStrategy
import io.netty.channel.SingleThreadEventLoop
import io.netty.util.IntSupplier
import io.netty.util.concurrent.RejectedExecutionHandler
import io.netty.util.concurrent.AbstractScheduledEventExecutor
import io.netty.util.internal.ObjectUtil
import io.netty.util.internal.PlatformDependent
import io.netty.util.internal.logging.InternalLogger
import io.netty.util.internal.logging.InternalLoggerFactory
import io.netty.util.internal.SystemPropertyUtil

import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants.STATUS

object UcxAmId {
    final val CONNECT = 0
    final val MESSAGE = 2
}

/**
 * {@link EventLoop} which uses epoll under the covers. Only works on Linux!
 */
class UcxEventLoop(parent: EventLoopGroup, executor: Executor,
    strategy: SelectStrategy, rejectedExecutionHandler: RejectedExecutionHandler,
    queueFactory: EventLoopTaskQueueFactory, val ucpContext: UcpContext)
    extends SingleThreadEventLoop(
        parent, executor, false,
        UcxEventLoop.newTaskQueue(queueFactory),
        UcxEventLoop.newTaskQueue(queueFactory),
        rejectedExecutionHandler) with UcxLogging {
    logDev(s"UcxEventLoop() parent $parent executor $executor ucpContext $ucpContext")

    private val ucxChannels = new ConcurrentHashMap[Long, AbstractUcxChannel]

    private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

    private[ucx] var ucpWorker: UcpWorker = _
    private[ucx] var ucpWorkerId: Long = _
    private[ucx] var ucpWorkerFd: Int = _

    def ucxEventLoopGroup = parent.asInstanceOf[UcxEventLoopGroup]

    private[ucx] def createUcpWorker(ucpWorkerParams: UcpWorkerParams): UcpWorker = {
        val ucpWorker = ucpContext.newWorker(ucpWorkerParams)

        ucpWorker.setAmRecvHandler(
            UcxAmId.CONNECT,
            new UcpAmRecvCallback {
                override def onReceive(
                    headerAddress: Long, headerSize: Long, amData: UcpAmData,
                    ep: UcpEndpoint): Int = {
                        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                        val nativeId = ep.getNativeId()
                        val remoteId = header.getLong
                        val channel = ucxChannels.get(nativeId)

                        channel.ucxHandleConnect(ep, remoteId)
                        STATUS.UCS_OK
                    }
            },
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

        ucpWorker.setAmRecvHandler(
            UcxAmId.MESSAGE,
            new UcpAmRecvCallback {
                override def onReceive(
                    headerAddress: Long, headerSize: Long, amData: UcpAmData,
                    ep: UcpEndpoint): Int = {
                        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                        val uniqueId = header.getLong
                        val channel = ucxChannels.get(uniqueId)

                        channel.ucxRead(amData)
                        STATUS.UCS_OK
                    }
            },
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

        ucpWorker
    }

    /**
     * Register the given channel with this {@link EventLoop}.
     */
    def addChannel(ch: AbstractUcxChannel): AbstractUcxChannel = {
        addChannel(ch.ucxUnsafe.uniqueId.get, ch)
    }

    /**
     * Register the given channel with this {@link EventLoop}.
     */
    def addChannel(id: Long, ch: AbstractUcxChannel): AbstractUcxChannel = {
        ucxChannels.putIfAbsent(id, ch)
    }

    /**
     * Deregister the given channel from this {@link EventLoop}.
     */
    def delChannel(ch: AbstractUcxChannel): AbstractUcxChannel = {
        delChannel(ch.ucxUnsafe.uniqueId.get)
    }

    /**
     * Deregister the given channel from this {@link EventLoop}.
     */
    def delChannel(id: Long): AbstractUcxChannel = {
        ucxChannels.remove(id)
    }

    private final var eventFd: Int = -1
    private final var epollFd: Int = -1
    private final val channels = new scala.collection.concurrent.TrieMap[
        InetSocketAddress, AbstractUcxChannel]
    private final var events: NativeEpollEventArray = _

    // // These are initialized on first use
    // private var iovArray: IovArray = _
    // // private var datagramPacketArray: NativeDatagramPacketArray = _

    private final var selectStrategy: SelectStrategy = _
    private final val selectNowSupplier: IntSupplier = new IntSupplier() {
        override def get(): Int = {
            epollWaitNow()
        }
    }

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final val nextWakeupNanos = new AtomicLong(AWAKE)
    private var pendingWakeup: Boolean = true
    @volatile private var ioRatio = 50

    private final val AWAKE = -1L
    private final val NONE = Long.MaxValue
    // See http://man7.org/linux/man-pages/man2/timerfd_create.2.html.
    private final val MAX_SCHEDULED_TIMERFD_NS = 999999999

    {
        selectStrategy = ObjectUtil.checkNotNull(strategy, "strategy")
        events = new NativeEpollEventArray(4)
        var success = false
        try {
            ucpWorker = createUcpWorker(ucpWorkerParams)
            ucpWorkerId = ucpWorker.getNativeId()
            ucpWorkerFd = ucpWorker.getEventFD()

            epollFd = NativeEpoll.newEpoll()
            try {
                ucpWorker.arm()
            } catch  {
                case e: UcxException => {
                    logDev("worker arm:", e)
                    pendingWakeup = false
                }
                case e: Throwable =>
                throw new IllegalStateException("worker arm:", e)
            }
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call read(...).
                NativeEpoll.epollCtlAdd(epollFd, ucpWorkerFd, NativeEpoll.EPOLLIN)
            } catch  {
                case e: IOException =>
                throw new IllegalStateException("Unable to add workerFd filedescriptor to epoll", e)
            }
            eventFd = NativeEpoll.newEventFd()
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call eventfd_read(...).
                NativeEpoll.epollCtlAdd(epollFd, eventFd, NativeEpoll.EPOLLIN | NativeEpoll.EPOLLET)
            } catch {
                case e: IOException =>
                throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e)
            }
            success = true
        } finally {
            if (!success) {
                if (epollFd > 0) {
                    try {
                        NativeEpoll.close(epollFd)
                    } catch {
                        // ignore
                        case _: Exception => {}
                    }
                }
                if (eventFd > 0) {
                    try {
                        NativeEpoll.close(eventFd)
                    } catch {
                        // ignore
                        case _: Exception => {}
                    }
                }
            }
        }
    }

    override
    protected def wakeup(inEventLoop: Boolean): Unit = {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            // write to the evfd which will then wake-up epoll_wait(...)
            pendingWakeup = false
            NativeEpoll.eventFdWrite(eventFd, 1L)
        }
    }

    override
    protected def beforeScheduledTaskSubmitted(deadlineNanos: Long): Boolean = {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get()
    }

    override
    protected def afterScheduledTaskSubmitted(deadlineNanos: Long): Boolean = {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get()
    }

    override
    protected def newTaskQueue(maxPendingTasks: Int): Queue[Runnable] = {
        // This event loop never calls takeTask()
        if (maxPendingTasks == Integer.MAX_VALUE)
            PlatformDependent.newMpscQueue[Runnable]()
        else
            PlatformDependent.newMpscQueue[Runnable](maxPendingTasks)
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    def getIoRatio(): Int = {
        return ioRatio
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    def setIoRatio(ioRatio: Int): Unit = {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)")
        }
        this.ioRatio = ioRatio
    }

    override
    def registeredChannels(): Int = {
        return channels.size
    }

    private def epollWait(deadlineNanos: Long): Int = {
        if (deadlineNanos == NONE) {
            return NativeEpoll.epollWait(epollFd, events, Int.MaxValue) // disarm timer
        }
        val totalDelay = AbstractScheduledEventExecutor.deadlineToDelayNanos(deadlineNanos)
        val delayMillis = totalDelay / 1000000
        return NativeEpoll.epollWait(epollFd, events, delayMillis.toInt)
    }

    private def epollWaitNoTimerChange(): Int = {
        return NativeEpoll.epollWait(epollFd, events)
    }

    private def epollWaitTimeboxed(): Int = {
        return NativeEpoll.epollWait(epollFd, events, 1000)
    }

    private def epollWaitNow(): Int = {
        return NativeEpoll.epollNow(epollFd, events)
    }

    private def epollBusyWait(): Int = {
        return NativeEpoll.epollPolling(epollFd, events)
    }

    override
    protected def run(): Unit = {
        UcxEventLoop.localWorker.set(this)
        while (true) {
            try {
                var strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks())
                strategy match {
                    case SelectStrategy.CONTINUE => {}

                    case SelectStrategy.BUSY_WAIT => {
                        strategy = epollBusyWait()
                    }

                    case SelectStrategy.SELECT => {
                        try {
                            if (pendingWakeup) {
                                var curDeadlineNanos = nextScheduledTaskDeadlineNanos()
                                if (curDeadlineNanos == -1L) {
                                    curDeadlineNanos = NONE // nothing on the calendar
                                }
                                nextWakeupNanos.set(curDeadlineNanos)
                                strategy = epollWait(curDeadlineNanos)
                            }
                        } finally {
                            // Try get() first to avoid much more expensive CAS in the case we
                            // were woken via the wakeup() method (submitted task)
                            if (nextWakeupNanos.get() != AWAKE) {
                                nextWakeupNanos.set(AWAKE)
                            }
                        }
                    }
                    case _ => {}
                }
                // we have only 2 fds: event fd and worker fd.
                val hasIOReady = (!pendingWakeup) || (strategy > 1) ||
                                 ((strategy == 1) && (events.fd(0) == ucpWorkerFd))
                val ioRatio = this.ioRatio
                if (ioRatio == 100) {
                    try {
                        processReady()
                    } finally {
                        // Ensure we always run tasks.
                        runAllTasks()
                    }
                } else if (hasIOReady) {
                    val ioStartTime = System.nanoTime()
                    try {
                        processReady()
                    } finally {
                        // Ensure we always run tasks.
                        val ioTime = System.nanoTime() - ioStartTime
                        runAllTasks(ioTime * (100 - ioRatio) / ioRatio)
                    }
                } else {
                    // run least task
                    runAllTasks(0)
                }

                if (isShuttingDown()) {
                    closeAll()
                    if (confirmShutdown()) {
                        return
                    }
                }
            } catch {
                case t: Throwable => handleLoopException(t)
            }
        }
    }

    /**
     * Visible only for testing!
     */
    def handleLoopException(t: Throwable) = {
        logWarning("Unexpected exception in the selector loop.", t)

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000)
        } catch {
            case e: InterruptedException => {}// Ignore.
        }
    }

    private def closeAll() = {
        // Using the intermediate collection to prevent ConcurrentModificationException.
        // In the `close()` method, the channel is deleted from `channels` map.
        channels.values.foreach(_.close())
        if (ucpWorker != null) {
            UcxEventLoop.localWorker.set(null)
            ucpWorker.close()
            ucpWorker = null
        }
    }

    // Returns true if a timerFd event was encountered
    private def processReady(): Unit = {
        while (ucpWorker.progress() != 0) {}

        pendingWakeup = (NativeEpoll.ucpWorkerArm(ucpWorkerId) == STATUS.UCS_OK)
    }

    override
    protected def cleanup() = {
        try {
            // Ensure any in-flight wakeup writes have been performed prior to closing eventFd.
            var hasInFlight = (!pendingWakeup) || (epollWaitNow() > 0)
            while (hasInFlight) {
                try {
                    processReady()
                    hasInFlight = (!pendingWakeup) || (epollWaitNow() > 0)
                } catch {
                    // ignore
                    case _: IOException => {}
                }
            }

            try {
                NativeEpoll.close(eventFd)
            } catch {
                case e: Exception =>
                    logWarning("Failed to close the timer fd.", e)
            }

            try {
                NativeEpoll.close(epollFd)
            } catch {
                case e: Exception =>
                logWarning("Failed to close the epoll fd.", e)
            }
        } finally {
            events.close()
        }
    }
}

object UcxEventLoop {
    final val localWorker = new ThreadLocal[UcxEventLoop]

    final val DEFAULT_MAX_PENDING_TASKS = SystemPropertyUtil.getInt(
        "io.netty.eventLoop.maxPendingTasks", Integer.MAX_VALUE).max(16)

    def newTaskQueue(queueFactory: EventLoopTaskQueueFactory): Queue[Runnable] = {
        if (queueFactory == null) 
            PlatformDependent.newMpscQueue[Runnable](DEFAULT_MAX_PENDING_TASKS)
        else
            queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS)
    }
}