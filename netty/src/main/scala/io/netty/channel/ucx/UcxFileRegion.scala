package io.netty.channel.ucx

import io.netty.util.AbstractReferenceCounted
import io.netty.util.IllegalReferenceCountException
import io.netty.util.internal.ObjectUtil
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.UcxPooledByteBufAllocator
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel

/**
 * Ucx implementation of {@link FileRegion} which transfer data from a {@link FileChannel} or {@link File}.
 *
 * Be aware that the {@link FileChannel} will be automatically closed once {@link #refCnt()} returns
 * {@code 0}.
 *
 * mmap is slower than read in pipeline.
 */
@Deprecated
class UcxFileRegion(protected val f: File, protected var offset: Long,
                    protected var length: Long)
    extends AbstractReferenceCounted with FileRegion with UcxLogging {

    protected var xferred: Long = 0
    protected var file: FileChannel = null
    protected var region: DefaultFileRegion = null

    protected var fd: Int = -1
    protected var mmapPtr: Long = 0l

    def this(fr: DefaultFileRegion) = {
        this(UcxFileRegion.getFile(fr), fr.position(), fr.count())
        xferred = fr.transferred()
        if ((f != null) && (length != 0)) {
            val aligned = MmapUtils.alignDown(offset, UcxFileRegion.PAGE_SIZE)
            val padding = offset - aligned
            xferred += padding
            length += padding
            offset = aligned
            fr.release()
        } else {
            region = fr
        }
    }

    def getMmap(): Long = {
        if (mmapPtr != 0l) {
            return mmapPtr
        }

        fd = NativeEpoll.open(f.toString(), UcxFileRegion.O_RDWR)
        mmapPtr = NativeEpoll.mmap(0, length, UcxFileRegion.MAP_PROT,
                                   UcxFileRegion.MAP_FLAG, fd, offset)
        return mmapPtr
    }

    def getChannel(): FileChannel = {
        if (file != null) {
            return file
        }

        if (f != null) {
            file = new RandomAccessFile(f, "rw").getChannel()
            return file
        }

        file = UcxFileRegion.getChannel(region)
        return file
    }

    // ucx need rw permission.
    def canMmap(): Boolean = f != null

    override
    def position(): Long = offset

    override
    def count(): Long = length

    override
    def transfered(): Long = xferred

    override
    def transferred(): Long = xferred

    override
    def transferTo(target: WritableByteChannel, offset: Long): Long = {
        val remain = length - offset
        if (remain < 0 || offset < 0 || refCnt() == 0) {
            throw new IllegalArgumentException(
                    s"offset ${offset} remain ${remain} refCnt ${refCnt()}")
        }
        if (remain == 0) {
            return 0L
        }

        val written = getChannel().transferTo(this.offset + offset, remain, target)
        if (written > 0) {
            xferred += written
        } else if (written == 0) {
            // If the amount of written data is 0 we need to check if the requested length is bigger then the
            // actual file itself as it may have been truncated on disk.
            //
            // See https://github.com/netty/netty/issues/8868
            validate(this, offset)
        }
        return written
    }

    override
    protected def deallocate(): Unit = {
        val mmapPtr = this.mmapPtr
        if (mmapPtr != 0l) {
            this.mmapPtr = 0l
            try {
                NativeEpoll.close(fd)
                NativeEpoll.munmap(mmapPtr, length)
            } catch {
                case e: IOException => logWarning("Failed to unmmap.", e)
            }
        }

        val region = this.region
        if (region != null) {
            this.region = null
            try {
                region.release()
            } catch {
                case e: IOException => logWarning("Failed to release region.", e)
            }
        }

        val file = this.file
        if (file != null) {
            this.file = null
            try {
                file.close()
            } catch {
                case e: IOException => logWarning("Failed to close file.", e)
            }
        }
    }

    override
    def retain(): FileRegion = {
        super.retain()
        this
    }

    override
    def retain(increment: Int): FileRegion = {
        super.retain(increment)
        this
    }

    override
    def touch(): FileRegion = this

    override
    def touch(hint: Object): FileRegion = this

    override
    def toString(): String = {
        s"UcxFileRegion($f, $offset $length($xferred))"
    }

    def validate(region: UcxFileRegion, offset: Long): Unit = {
        val size = region.file.size();
        val count = region.count - position;
        if (region.position + count + position > size) {
            throw new IOException("Underlying file size " + size + " smaller then requested count " + region.count);
        }
    }
}

object UcxFileRegion {
    private val clazz = classOf[DefaultFileRegion]
    private val fileField = clazz.getDeclaredField("file")
    private val fField = clazz.getDeclaredField("f")
    val O_RDWR = NativeEpoll.O_RDWR
    val MAP_PROT = NativeEpoll.PROT_READ | NativeEpoll.PROT_WRITE
    val MAP_FLAG = NativeEpoll.MAP_SHARED
    val PAGE_SIZE = 4096l // PooledByteBufAllocator.defaultPageSize()

    fileField.setAccessible(true)
    fField.setAccessible(true)

    def getChannel(fr: DefaultFileRegion): FileChannel = {
        fr.open()
        return fileField.get(fr).asInstanceOf[FileChannel]
    }

    def getFile(fr: DefaultFileRegion): File = {
        return fField.get(fr).asInstanceOf[File]
    }

    def copyDefaultFileRegion(fileChannel: FileChannel, offset: Long,
                              length: Long, directBuf: ByteBuf): Unit = {
        val mapBuf = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                                     offset, length)
        directBuf.writeBytes(mapBuf)
    }

    def readDefaultFileRegion(fileChannel: FileChannel, offset: Long,
                              length: Long, directBuf: ByteBuf): Unit = {
        directBuf.writeBytes(fileChannel, offset, length.toInt)
    }
}