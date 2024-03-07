package io.netty.channel.ucx

import org.openucx.jucx.{NativeLibs, UcxException}
import org.openucx.jucx.ucp._
import org.openucx.jucx.UcxCallback
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import io.netty.channel.EventLoop
import io.netty.channel.EventLoopGroup
import io.netty.channel.EventLoopTaskQueueFactory
import io.netty.channel.SelectStrategy
import io.netty.channel.SingleThreadEventLoop
import io.netty.channel.unix.IovArray
import io.netty.util.IntSupplier
import io.netty.util.collection.IntObjectHashMap
import io.netty.util.collection.IntObjectMap
import io.netty.util.concurrent.RejectedExecutionHandler
import io.netty.util.concurrent.AbstractScheduledEventExecutor
import io.netty.util.internal.ObjectUtil
import io.netty.util.internal.PlatformDependent
import io.netty.util.internal.logging.InternalLogger
import io.netty.util.internal.logging.InternalLoggerFactory
import io.netty.util.internal.SystemPropertyUtil

import java.io.IOException
import java.nio.ByteBuffer
import java.net.InetSocketAddress
import java.util.Queue
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap

object UcxAmId {
    final val CONNECT = 0
    final val REPLY_CONNECT = 1
    final val MESSAGE = 2
}

/**
 * {@link EventLoop} which uses epoll under the covers. Only works on Linux!
 */
class UcxEventLoop(
    parent: EventLoopGroup, executor: Executor, maxEvents: Int,
    strategy: SelectStrategy, rejectedExecutionHandler: RejectedExecutionHandler,
    queueFactory: EventLoopTaskQueueFactory)
    extends SingleThreadEventLoop(
        parent, executor, false,
        UcxEventLoop.newTaskQueue(queueFactory),
        UcxEventLoop.newTaskQueue(queueFactory),
        rejectedExecutionHandler) with UcxLogging {

    private val ucxChannels = new ConcurrentHashMap[Long, AbstractUcxChannel]

    private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
            .requestWakeupRX().requestWakeupTX().requestWakeupEdge()

    private[ucx] var ucpWorker: UcpWorker = _
    private[ucx] var ucpWorkerFd: Int = _

    def ucxEventLoopGroup = parent.asInstanceOf[UcxEventLoopGroup]

    private[ucx] def createUcpWorker(ucpWorkerParams: UcpWorkerParams): UcpWorker = {
        val ucpWorker = ucxEventLoopGroup.ucpContext.newWorker(ucpWorkerParams)

        ucpWorker.setAmRecvHandler(
            UcxAmId.CONNECT,
            new UcpAmRecvCallback {
                override def onReceive(
                    headerAddress: Long, headerSize: Long, amData: UcpAmData,
                    epIn: UcpEndpoint): Int = {
                        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                        val address = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
                        val nativeId = epIn.getNativeId()
                        val remoteId = header.getLong
                        val channel = ucxChannels.get(nativeId)

                        channel.ucxConnectBack(remoteId, address)
                        UcsConstants.STATUS.UCS_OK
                    }
            },
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

        ucpWorker.setAmRecvHandler(
            UcxAmId.REPLY_CONNECT,
            new UcpAmRecvCallback {
                override def onReceive(
                    headerAddress: Long, headerSize: Long,
                    ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
                        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                        val uniqueId = header.getLong
                        val remoteId = header.getLong
                        val channel = ucxChannels.get(uniqueId)

                        channel.ucxConnectDone(remoteId)
                        UcsConstants.STATUS.UCS_OK
                    }
            },
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

        ucpWorker.setAmRecvHandler(
            UcxAmId.MESSAGE,
            new UcpAmRecvCallback {
                override def onReceive(
                    headerAddress: Long, headerSize: Long,
                    ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
                        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                        val uniqueId = header.getLong
                        val channel = ucxChannels.get(uniqueId)

                        channel.ucxRead(ucpAmData)
                        UcsConstants.STATUS.UCS_OK
                    }
            },
            UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

        ucpWorker
    }

    /**
     * Register the given channel with this {@link EventLoop}.
     */
    def addChannel(ch: AbstractUcxChannel): Unit = {
        addChannel(ch.ucxUnsafe.uniqueId.get, ch)
    }

    /**
     * Register the given channel with this {@link EventLoop}.
     */
    def addChannel(id: Long, ch: AbstractUcxChannel): Unit = {
        ucxChannels.putIfAbsent(id, ch)
    }

    /**
     * Deregister the given channel from this {@link EventLoop}.
     */
    def delChannel(ch: AbstractUcxChannel): Unit = {
        delChannel(ch.ucxUnsafe.uniqueId.get)
    }

    /**
     * Deregister the given channel from this {@link EventLoop}.
     */
    def delChannel(id: Long): Unit = {
        ucxChannels.remove(id)
    }

    private final var eventFd: Int = -1
    private final var epollFd: Int = -1
    private final var timerFd: Int = -1
    private final val channels = new scala.collection.concurrent.TrieMap[
        InetSocketAddress, AbstractUcxChannel]
    private final var allowGrowing: Boolean = _
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
    private var pendingWakeup: Boolean = _
    @volatile private var ioRatio = 50

    private final val AWAKE = -1L
    private final val NONE = Long.MaxValue
    // See http://man7.org/linux/man-pages/man2/timerfd_create.2.html.
    private final val MAX_SCHEDULED_TIMERFD_NS = 999999999

    {
        selectStrategy = ObjectUtil.checkNotNull(strategy, "strategy")
        if (maxEvents == 0) {
            allowGrowing = true
            events = new NativeEpollEventArray(4)
        } else {
            allowGrowing = false
            events = new NativeEpollEventArray(maxEvents)
        }
        var success = false
        try {
            ucpWorker = createUcpWorker(ucpWorkerParams)
            ucpWorkerFd = ucpWorker.getEventFD()

            epollFd = NativeEpoll.newEpoll()
            eventFd = ucpWorkerFd
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call eventfd_read(...).
                NativeEpoll.epollCtlAdd(epollFd, eventFd, NativeEpoll.EPOLLIN | NativeEpoll.EPOLLET)
            } catch {
                case e: IOException =>
                throw new IllegalStateException("Unable to add eventFd filedescriptor to epoll", e)
            }
            timerFd = NativeEpoll.newTimerFd()
            try {
                // It is important to use EPOLLET here as we only want to get the notification once per
                // wakeup and don't call read(...).
                NativeEpoll.epollCtlAdd(epollFd, timerFd, NativeEpoll.EPOLLIN | NativeEpoll.EPOLLET)
            } catch  {
                case e: IOException =>
                throw new IllegalStateException("Unable to add timerFd filedescriptor to epoll", e)
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
                if (timerFd > 0) {
                    try {
                        NativeEpoll.close(timerFd)
                    } catch {
                        // ignore
                        case _: Exception => {}
                    }
                }
            }
        }
    }

    // /**
    //  * Return a cleared {@link IovArray} that can be used for writes in this {@link EventLoop}.
    //  */
    // def cleanIovArray(): IovArray = {
    //     if (iovArray == null) {
    //         iovArray = new IovArray()
    //     } else {
    //         iovArray.clear()
    //     }
    //     return iovArray
    // }

    // /**
    //  * Return a cleared {@link NativeDatagramPacketArray} that can be used for writes in this {@link EventLoop}.
    //  */
    // def cleanDatagramPacketArray(): NativeDatagramPacketArray = {
    //     if (datagramPacketArray == null) {
    //         datagramPacketArray = new NativeDatagramPacketArray()
    //     } else {
    //         datagramPacketArray.clear()
    //     }
    //     return datagramPacketArray
    // }

    override
    protected def wakeup(inEventLoop: Boolean): Unit = {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            // write to the evfd which will then wake-up epoll_wait(...)
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
        val delayMillis = totalDelay / 1000
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
        var prevDeadlineNanos = NONE
        UcxEventLoop.localWorker.set(this)
        while (true) {
            try {
                var strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks())
                strategy match {
                    case SelectStrategy.CONTINUE => {}

                    case SelectStrategy.BUSY_WAIT => while (ucpWorker.progress == 0) {}

                    case SelectStrategy.SELECT => {
                        var skip = false
                        if (pendingWakeup) {
                            // We are going to be immediately woken so no need to reset wakenUp
                            // or check for timerfd adjustment.
                            strategy = epollWaitTimeboxed()
                            if (strategy != 0) {
                                skip = true
                            } else {
                                // We timed out so assume that we missed the write event due to an
                                // abnormally failed syscall (the write itself or a prior epoll_wait)
                                logWarning("Missed eventfd write (not seen after > 1 second)")
                                pendingWakeup = false
                                if (hasTasks()) {
                                    skip = true
                                }
                            }
                        }

                        if (!skip) {
                            var curDeadlineNanos = nextScheduledTaskDeadlineNanos()
                            if (curDeadlineNanos == -1L) {
                                curDeadlineNanos = NONE // nothing on the calendar
                            }
                            nextWakeupNanos.set(curDeadlineNanos)
                            try {
                                if (!hasTasks()) {
                                    if (curDeadlineNanos == prevDeadlineNanos) {
                                        // No timer activity needed
                                        strategy = epollWaitNoTimerChange()
                                    } else {
                                        // Timerfd needs to be re-armed or disarmed
                                        prevDeadlineNanos = curDeadlineNanos
                                        strategy = epollWait(curDeadlineNanos)
                                    }
                                }
                            } finally {
                                // Try get() first to avoid much more expensive CAS in the case we
                                // were woken via the wakeup() method (submitted task)
                                if (nextWakeupNanos.get() == AWAKE || nextWakeupNanos.getAndSet(AWAKE) == AWAKE) {
                                    pendingWakeup = true
                                }
                            }
                        }
                    }
                    case _ => {}
                }

                val ioRatio = this.ioRatio
                if (ioRatio == 100) {
                    try {
                        if (strategy > 0 && processReady(strategy)) {
                            prevDeadlineNanos = NONE
                        }
                    } finally {
                        // Ensure we always run tasks.
                        runAllTasks()
                    }
                } else if (strategy > 0) {
                    val ioStartTime = System.nanoTime()
                    try {
                        if (processReady(strategy)) {
                            prevDeadlineNanos = NONE
                        }
                    } finally {
                        // Ensure we always run tasks.
                        val ioTime = System.nanoTime() - ioStartTime
                        runAllTasks(ioTime * (100 - ioRatio) / ioRatio)
                    }
                } else {
                    runAllTasks(0) // This will run the minimum number of tasks
                }

                if (allowGrowing && strategy == events.length()) {
                    //increase the size of the array as we needed the whole space for the events
                    events.increase()
                }

                if (isShuttingDown()) {
                    closeAll()
                    UcxEventLoop.localWorker.set(null)
                    ucpWorker.close()
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
        ucpWorker.close()
    }

    // Returns true if a timerFd event was encountered
    private def processReady(ready: Int): Boolean = {
        var timerFired = false
        for (i <- 0 until ready) {
            val fd = events.fd(i)
            if (fd == eventFd) {
                ucpWorker.progress()
            } else if (fd == timerFd) {
                timerFired = true
            }
        }
        return timerFired
    }

    override
    protected def cleanup() = {
        try {
            // Ensure any in-flight wakeup writes have been performed prior to closing eventFd.
            while (pendingWakeup) {
                try {
                    while (ucpWorker.progress() != 0) {}
                    val count = epollWaitTimeboxed()
                    if (count == 0) {
                        pendingWakeup = false
                    }
                } catch {
                    // ignore
                    case _: IOException => {}
                }
            }

            try {
                NativeEpoll.close(timerFd)
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
    final val loadNative = UcxEventLoopGroup.loadNative
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