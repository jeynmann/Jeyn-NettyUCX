package org.apache.spark.ucx.dio

import java.io.File
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import io.netty.util.AbstractReferenceCounted
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.channel.ucx.UnsafeUtils
import io.netty.channel.ucx.UcxLogging

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.openucx.jucx.ucs.UcsConstants

import io.netty.channel.ucx.NativeEpoll;

class IOTask protected[dio]() {
    protected[dio] val op: AtomicInteger = new AtomicInteger()
    protected[dio] var fd: Int = 0
    protected[dio] var buf: Long = 0
    protected[dio] var len: Long = 0
    protected[dio] var pos: Long = 0

    def reset(fd: Int, buf: Long, len: Long, pos: Long): this.type = {
        this.fd = fd
        this.buf = buf
        this.len = len
        this.pos = pos
        this
    }

    def sync(): Unit = {
        if (!isComplete()) {
            do {
                // polling
            } while (!isComplete())
        }
    }

    def tryRead(): Boolean = {
        // val o = this.op.get()
        // (o != IOTask.OP_WRITE) && (
        //     (o == IOTask.OP_READ) ||
        //     (this.op.compareAndSet(IOTask.OP_FIN, IOTask.OP_READ)) ||
        //     (this.op.get() == IOTask.OP_READ)
        // )
        this.op.compareAndSet(IOTask.OP_FIN, IOTask.OP_READ)
    }

    def tryWrite(): Boolean = {
        this.op.compareAndSet(IOTask.OP_FIN, IOTask.OP_WRITE)
    }

    def isComplete(): Boolean = {
        op.get() == IOTask.OP_FIN
    }

    def complete(): Unit = {
        op.set(IOTask.OP_FIN)
    }
}

object IOTask {
    val DUMMY = new IOTask()

    val OP_FIN = 0
    val OP_READ = 1
    val OP_WRITE = 2
}

class IOWorker protected[dio](parent: IOService, queueDepth: Int) extends Thread {
    protected[dio] val parentStop = parent.stop
    protected[dio] val pendTask = new LinkedBlockingQueue[IOTask]()
    setDaemon(true)

    protected[dio] def wakeup(): Unit = {
        pendTask.offer(IOTask.DUMMY)
    }

    protected[dio] def post(task: IOTask): Unit = {
        pendTask.offer(task)
    }

    override def run(): Unit = {
        while (!parentStop.get()) {
            val task = pendTask.take()
            task.op.get() match {
                case IOTask.OP_READ => 
                    NativeEpoll.pread(task.fd, task.buf, task.len, task.pos)
                    task.complete()
                case IOTask.OP_WRITE =>
                    NativeEpoll.pwrite(task.fd, task.buf, task.len, task.pos)
                    task.complete()
                case _: Int => {}
            }
        }
    }
}

class IOService(numWorker: Int, queueDepth: Int) {
    protected[dio] val stop = new AtomicBoolean(false)
    protected[dio] val id = new AtomicInteger()
    protected[dio] val workers = new Array[IOWorker](numWorker)

    def post(task: IOTask): Unit = {
        workers(id.incrementAndGet().abs % numWorker).post(task)
    }

    def start(): Unit = {
        for (i <- 0 until workers.size) {
            workers(i) = new IOWorker(this, queueDepth)
            workers(i).start()
        }
    }

    def close(): Unit = {
        stop.set(true)
        workers.foreach(_.wakeup())
    }
}

object IOService {
    private var inst: IOService = _

    def instance() = inst

    def initialize(numWorker: Int, queueDepth: Int): Unit = {
        assert(inst == null)
        inst = new IOService(numWorker, queueDepth)
        inst.start()
    }
}

class PageCache protected[dio](val address: Long) {
    protected[dio] val refCnt = new AtomicInteger(1)
    protected[dio] val state = new AtomicInteger()
    protected[dio] val ioTask = new IOTask()
    protected[dio] var parent: FileCache = _
    protected[dio] var offset = 0l
    protected[dio] var length = 0

    protected[dio] def reset(f: FileCache, offset: Long, length: Long): this.type = {
        invalid()
        this.parent = f
        this.offset = offset
        this.length = length.toInt
        this
    }

    def getOffset(): Long = offset

    def getLength(): Int = length

    def read(fd: Int, pos: Long, len: Int): ByteBuf = {
        if (!isValid()) {
            readBackend(fd)
            ioTask.sync()
            syned()
        }
        // assert(pos + len <= offset + length)
        FileService.debug(s"${this.getClass} read${(fd, pos, len)}")
        return Unpooled.wrappedBuffer(address + pos, len, false)
    }

    def write(buf: ByteBuf, fd: Int, pos: Long, len: Int): Int = {
        val inner = UnsafeUtils.getByteBufferView(address + pos, len)
        val readerIndex = buf.readerIndex()
        if (!isValid()) {
            dirty()
        }
        // assert(pos + len <= offset + length)
        FileService.debug(s"${this.getClass} write${(fd, pos, len)}")
        buf.readBytes(inner)
        buf.readerIndex() - readerIndex
    }

    def readBackend(fd: Int): Unit = {
        if (ioTask.tryRead()) {
            ioTask.reset(fd, address, length, offset)
            FileService.debug(s"${this.getClass} readBackend${(fd, address, length, offset)}")
            IOService.instance().post(ioTask)
        }
    }

    def writeBackend(fd: Int): Unit = {
        if (ioTask.tryWrite()) {
            ioTask.reset(fd, address, length, offset)
            FileService.debug(s"${this.getClass} writeBackend${(fd, address, length, offset)}")
            IOService.instance().post(ioTask)
        }
    }

    def retain(fd: Int): Int = {
        FileService.debug(s"${this.getClass} retain${(fd)}")
        refCnt.incrementAndGet()
    }

    def release(fd: Int): Int = {
        FileService.debug(s"${this.getClass} release${(fd)}")
        val rc = refCnt.decrementAndGet()
        if (rc == 1) {
            if (isDirty()) {
                writeBackend(fd)
                ioTask.sync()
                syned()
            }
        }
        rc
    }

    def dirty() = {
        state.set(PageCache.ST_DIRTY)
    }

    def syned() = {
        state.set(PageCache.ST_SYNED)
    }

    def invalid() = {
        state.set(PageCache.ST_INVALID)
    }

    def isValid() = {
        state.get() != PageCache.ST_INVALID
    }

    def isDirty() = {
        state.get() == PageCache.ST_DIRTY
    }

    def isIOComplete() = {
        ioTask.isComplete()
    }
}

object PageCache {
    def pageSize(): Int = pgSize

    def blockSize(): Int = blkSize

    def pageNum(): Int = blockSize() / pageSize()

    def alignDown(pos: Long, align: Long): Long = {
        pos & ~(align - 1)
    }

    def alignUp(pos: Long, align: Long): Long = {
        alignDown(pos + align - 1, align)
    }

    def allocate(): PageCache = {
        val nowId = memId.incrementAndGet()
        val gId = nowId / blockSize()
        val mId = nowId % blockSize()
        new PageCache(alignedPtrs(gId) + mId * pageSize())
    }

    def initialize(ucpContext: UcpContext, pageSize: Int, blockSize: Int, blockNum: Int): Unit = {
        assert(ucpParams == null)
        pgSize = pageSize
        blkSize = blockSize
        ucpCtx = ucpContext
        ucpParams = new UcpMemMapParams().allocate().nonBlocking()
            .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            .setLength(blockSize + pageSize)
        ucpMems = new Array[UcpMemory](blockNum)
        alignedPtrs = new Array[Long](blockNum)
        for (i <- 0 until blockNum) {
            ucpMems(i) = ucpCtx.memoryMap(ucpParams)
            alignedPtrs(i) = alignUp(ucpMems(i).getAddress(), pageSize)
        }
    }

    def close(): Unit = {
        ucpMems.foreach(_.deregister())
    }

    val ST_INVALID = 0
    val ST_DIRTY = 1
    val ST_SYNED = 2

    val SZ_PAGE = 64 << 10
    val SZ_BLOCK = 2048 << 10

    private val memId = new AtomicInteger(-1)
    private var ucpParams: UcpMemMapParams = _ 
    private var ucpMems: Array[UcpMemory] = _
    private var alignedPtrs: Array[Long] = _
    private var ucpCtx: UcpContext = _
    private var blkSize: Int = SZ_BLOCK
    private var pgSize: Int = SZ_PAGE
}

// TODO: read/write conflicts
// TODO: partial retain/release
class FileCache protected[dio](parent: FileService, val path: String) {
    protected[dio] val refCnt = new AtomicInteger(1)
    protected[dio] val pages = new ConcurrentHashMap[Long, PageCache]

    def prepare(fd: Int, flag: Int, pos: Long, len: Int): Seq[PageCache] = {
        val pageSize = PageCache.pageSize().toLong
        val alignPos = PageCache.alignDown(pos, pageSize)
        val alignEnd = PageCache.alignUp(pos + len, pageSize)
        val positions = alignPos until alignEnd by pageSize

        FileService.debug(s"${this.getClass} prepare${(fd, pos, len)}")
        FileService.debug(s"${this.getClass} prepare${(fd, pageSize, alignPos, alignEnd, positions)}")
        positions.map(p => {
            pages.computeIfAbsent(p, p => {
                parent.allocate().reset(this, p, pageSize)
            })
        })
    }

    def retain(): Int = {
        FileService.debug(s"${this.getClass} retain")
        refCnt.incrementAndGet()
    }

    def release(): Int = {
        FileService.debug(s"${this.getClass} release")
        val rc = refCnt.decrementAndGet()
        if (rc == 0) {
            deallocate()
            return rc
        }
        if (rc == 1) {
            parent.release(this)
            return rc
        }
        return rc
    }

    def deallocate(): Unit = {
        FileService.debug(s"${this.getClass} deallocate")
        pages.values().forEach(parent.deallocate(_))
        pages.clear()
    }
}

class FileHandle protected[dio] (val path: String, val flag: Int, cache: FileCache)
    extends Closeable {
    protected[dio] val fd = NativeEpoll.open(path, flag)
    protected[dio] var size = NativeEpoll.statSize(fd)
    protected[dio] var pages: Seq[PageCache] = _

    cache.retain()

    def getFd(): Int = fd

    def getSize(): Long = size

    def read(pos: Long, len: Int): ByteBuf = {
        prepare(pos, len)
        if (pages.isEmpty) {
            return Unpooled.EMPTY_BUFFER
        }

        if (pages.size == 1) {
            return pages.head.read(fd, pos, len)
        }

        val alloc = UcxPooledByteBufAllocator.DEFAULT
        val buf = new CompositeByteBuf(alloc, true, pages.size)

        val head = pages.head
        val headPos = pos - head.getOffset()
        val headLen = head.getLength() - headPos.toInt

        val last = pages.last
        val lastLen = (pos + len - last.getOffset()).toInt

        buf.addComponent(head.read(fd, headPos, headLen.toInt))
        pages.foreach(b => {
            if ((b != head) && (b != last)) {
                buf.addComponent(b.read(fd, 0, b.getLength()))
            }
        })
        buf.addComponent(last.read(fd, 0, lastLen))
        FileService.debug(s"${this.getClass} read${(buf)}")

        buf
    }

    def write(buf: ByteBuf, pos: Long, len: Int): Int = {
        prepare(pos, len)
        if (pages.isEmpty) {
            return 0
        }

        if (pages.size == 1) {
            size = size.max(pos + len)
            return pages.head.write(buf, fd, pos, len)
        }

        var written = 0

        val head = pages.head
        val headPos = pos - head.getOffset()
        val headLen = head.getLength() - headPos.toInt

        val last = pages.last
        val lastLen = (pos + len - last.getOffset()).toInt

        written += head.write(buf, fd, headPos, headLen.toInt)
        pages.foreach(b => {
            if ((b != head) && (b != last)) {
                written += b.write(buf, fd, 0, b.getLength())
            }
        })
        written += last.write(buf, fd, 0, lastLen)
        FileService.debug(s"${this.getClass} write${(buf)}")

        size = size.max(pos + len)
        return written
    }

    protected[dio] def prepare(pos: Long, len: Int): Unit = {
        val oldPages = pages
        pages = cache.prepare(fd, flag, pos, len)
        pages.foreach(_.retain(fd))
        flush(oldPages)
    }

    protected[dio] def flush(p: Seq[PageCache]): Unit = {
        if (p != null) {
            p.foreach(_.release(fd))
        }
    }

    override def close(): Unit = {
        flush(pages)
        if (flag != FileHandle.O_READ_ONLY) {
            NativeEpoll.ftruncate(fd, size)
        }
        NativeEpoll.close(fd)
        cache.release()
    }
}

object FileHandle {
    val O_READ_ONLY = NativeEpoll.O_DIRECT | NativeEpoll.O_RDONLY
    val O_READ_WRITE = NativeEpoll.O_DIRECT | NativeEpoll.O_RDWR | NativeEpoll.O_CREAT
}

class FileService protected[dio](maxTotal: Long, maxCache: Long) {
    protected[dio] val stop = new AtomicBoolean(false)
    // protected[dio] val totalSize = new AtomicLong()
    protected[dio] val cacheSize = new AtomicLong()
    protected[dio] val files = new ConcurrentHashMap[String, FileCache]
    protected[dio] val nouse = new ConcurrentLinkedQueue[FileCache]
    protected[dio] val pages = new ConcurrentLinkedQueue[PageCache]

    def this() = {
        this(8L << 20, 7L << 20)
    }

    def this(maxTotal: Long) = {
        this(maxTotal, maxTotal * 7 / 8)
    }

    def initialize(): this.type = {
        for (i <- 0l until maxTotal by PageCache.pageSize().toLong) {
            pages.offer(PageCache.allocate())
        }
        this
    }

    def open(path: String, perm: String): FileHandle = {
        val flag = if (perm == "r") FileHandle.O_READ_ONLY else FileHandle.O_READ_WRITE
        val cache = files.computeIfAbsent(path, path => new FileCache(this, path))
        new FileHandle(path, flag, cache)
    }

    def allocate(): PageCache = {
        var b = pages.poll()
        if (b != null) {
            updateCacheSize(PageCache.pageSize())
            return b
        }

        do {
            doRecycle()
            b = pages.poll()
        } while (b == null)
        updateCacheSize(PageCache.pageSize())
        return b
    }

    def deallocate(p: PageCache): Unit = {
        pages.add(p)
        updateCacheSize(-PageCache.pageSize())
    }

    def release(f: FileCache): Unit = {
        nouse.offer(f)

        var sizeNow = cacheSize.get()
        if (sizeNow >= maxCache) {
            doRecycle()
        }
    }

    def doRecycle(): Unit = {
        val toFree = nouse.poll()
        if (toFree != null && toFree.refCnt.get() == 1) {
            files.remove(toFree.path)
            assert(toFree.release() == 0)
        }
    }

    def updateCacheSize(increment: Long): Unit = {
        var sizeNow = 0l
        do {
            sizeNow = cacheSize.get()
        } while (!cacheSize.compareAndSet(sizeNow, sizeNow + increment))
    }

    // def updateTotalSize(increment: Long) = {
    //     do {
    //         sizeNow = totalSize.get()
    //     } while (!totalSize.compareAndSet(sizeNow, sizeNow + increment))
    // }

    def close(): Unit = {
        stop.set(true)
        files.values().forEach(_.release())
        files.clear()
        nouse.forEach(_.release())
        nouse.clear()
        pages.clear()
    }
}

object FileService {
    private var inst: FileService = _

    def instance() = inst

    def open(path: String, perm: String): FileHandle = {
        inst.open(path, perm)
    }

    def close(f: FileCache): Unit = {
        f.release()
    }

    def close(): Unit = {
        inst.close()
    }

    def initialize(ucpContext: UcpContext, pageSize: Int, blockSize: Int, blockNum: Int,
                   numWorker: Int = 4, queueDepth: Int = 64): Unit = {
        PageCache.initialize(ucpContext, pageSize, blockSize, blockNum)
        IOService.initialize(numWorker, queueDepth)
        inst = new FileService(blockSize * blockNum).initialize()
    }

    def debug(msg: => String): Unit = {
        // println(s"[${System.currentTimeMillis()}] $msg")
    }

    def info(msg: => String): Unit = {
        println(s"[${System.currentTimeMillis()}] $msg")
    }

    def main(args: Array[String]): Unit = {
        println(s"[${System.currentTimeMillis()}] ${args.toSeq}")

        FileService.initialize(UcxPooledByteBufAllocator.UCP_CONTEXT,
                               64 << 10, 65536 << 10, 4)

        info(s"prepare..")
        val fSize = args(1).toInt
        val bSize = args(2).toInt

        val elem = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes()
        var bytes = elem
        while (bytes.size < bSize / 2) {
            bytes ++= bytes
        }
        while (bytes.size < bSize + elem.size) {
            bytes ++= elem
        }
        val wbuf = UcxPooledByteBufAllocator.DEFAULT.buffer(bSize)

        info(s"open..")
        val f0 = FileService.open(args(0), "rw")

        info(s"write..")
        var i = 0
        for (pos <- 0 until fSize by bSize) {
            i += 1
            wbuf.clear()
            wbuf.writeBytes(bytes, i % 36, bSize)
            info(s"pos ${pos}: ${wbuf.toString(0, 32, java.nio.charset.StandardCharsets.UTF_8)}")
            f0.write(wbuf, pos, wbuf.readableBytes())
            debug(s"pos ${pos}: ${f0.read(pos, bSize).toString(0, 32, java.nio.charset.StandardCharsets.UTF_8)}")
        }
        f0.close()

        info(s"open..")
        val f1 = FileService.open(args(0), "r")

        info(s"read..")
        for (pos <- 0 until fSize by bSize) {
            val buf = f1.read(pos, bSize)
            info(s"pos ${pos}: ${buf.toString(0, 32, java.nio.charset.StandardCharsets.UTF_8)}")
        }
        f1.close()
    }
}
