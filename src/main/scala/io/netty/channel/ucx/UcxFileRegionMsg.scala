package io.netty.channel.ucx

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion
import io.netty.util.AbstractReferenceCounted

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel

class UcxFileRegionMsg(val fr: FileRegion, val ucxChannel: UcxSocketChannel,
                       val offset: Long, val length: Long)
    extends AbstractReferenceCounted {

    protected val allocator = ucxChannel.config().getAllocator()

    val frameSize = ucxChannel.config().getFileFrameSize()
    val frameNums = (length.toInt - 1) / frameSize + 1
    protected var position = 0L
    protected var frameNow = 0

    def this(fr: FileRegion, ucxChannel: UcxSocketChannel) = {
        this(fr, ucxChannel, fr.transferred(), fr.count() - fr.transferred())
    }

    def isEmpty() = position == length

    def frameId() = frameNow

    def forEachFrame(processor: (Int, ByteBuf) => Unit, spinLimit: Int): Int = {
        val frameLimit = (frameNow + spinLimit).min(frameNums)
        while (frameNow != frameLimit) {
            val currentSize = frameSize.min((length - position).toInt)
            val byteChannel = new UcxWritableByteChannel(allocator, currentSize)

            fr.transferTo(byteChannel, position + offset)

            processor(frameNow, byteChannel.internalByteBuf())
            position += currentSize
            frameNow += 1
        }
        return frameLimit - frameNow
    }

    def forall(processor: ByteBuf => Unit): Unit = {
        val byteChannel = new UcxWritableByteChannel(allocator, length.toInt)

        fr.transferTo(byteChannel, offset)

        processor(byteChannel.internalByteBuf())
    }

    override
    def touch(): this.type = {
        return this
    }

    override
    def touch(hint: Object): this.type = {
        return this
    }

    override
    def deallocate(): Unit = {
        fr.release()
    }
}

class UcxDefaultFileRegionMsg(override val fr: DefaultFileRegion,
                              override val ucxChannel: UcxSocketChannel,
                              override val offset: Long,
                              override val length: Long)
    extends UcxFileRegionMsg(fr, ucxChannel, offset, length) {

    protected val fileChannel = UcxDefaultFileRegionMsg.getChannel(fr)

    def this(fr: DefaultFileRegion, ucxChannel: UcxSocketChannel) = {
        this(fr, ucxChannel, fr.position() + fr.transferred(),
             fr.count() - fr.transferred())
    }

    override def forEachFrame(processor: (Int, ByteBuf) => Unit, spinLimit: Int): Int = {
        val frameLimit = (frameNow + spinLimit).min(frameNums)
        while (frameNow != frameLimit) {
            val currentSize = frameSize.min((length - position).toInt)
            val directBuf = allocator.directBuffer(currentSize, currentSize)

            UcxDefaultFileRegionMsg.readDefaultFileRegion(
                fileChannel, position + offset, currentSize, directBuf)

            processor(frameNow, directBuf)
            position += currentSize
            frameNow += 1
        }
        return frameLimit - frameNow
    }

    override def forall(processor: ByteBuf => Unit): Unit = {
        val directBuf = allocator.directBuffer(length.toInt, length.toInt)

        UcxDefaultFileRegionMsg.readDefaultFileRegion(fileChannel, offset, length,
                                                      directBuf)
        processor(directBuf)
    }
}

object UcxDefaultFileRegionMsg {
    private val clazz = classOf[DefaultFileRegion]
    private val fileField = clazz.getDeclaredField("file")

    fileField.setAccessible(true)

    def getChannel(fr: DefaultFileRegion): FileChannel = {
        fr.open()
        return fileField.get(fr).asInstanceOf[FileChannel]
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

class UcxWritableByteChannel(val alloc: ByteBufAllocator, val size: Int)
    extends WritableByteChannel with UcxLogging {

    protected var opened = true
    protected var directBuf: ByteBuf = alloc.directBuffer(size, size)

    def internalByteBuf(): ByteBuf = directBuf

    override def write(src: ByteBuffer): Int = {
        val dup = src.duplicate()
        val readableBytes = src.remaining()
        if (readableBytes > size) {
            dup.limit(dup.position() + size)
        }

        directBuf.writeBytes(dup)

        src.position(dup.position())

        val written = readableBytes - src.remaining()
        logDev(s"write() $dup $src $written")
        return written
    }

    override def close(): Unit = {
        if (opened) {
            directBuf.release()
            opened = false
        }
    }

    override def isOpen() = opened
}

class UcxDummyWritableByteChannel(val size: Int)
    extends WritableByteChannel with UcxLogging {

    protected var opened = true

    override def write(src: ByteBuffer): Int = {
        val readableBytes = src.remaining().min(size)
        val position = src.position()
        src.position(position + readableBytes)
        return readableBytes
    }

    override def close(): Unit = {
        if (opened) {
            opened = false
        }
    }

    override def isOpen() = opened
}

object UcxDummyWritableByteChannel {
    def apply(ucxWritableCh: UcxWritableByteChannel): UcxDummyWritableByteChannel = {
        new UcxDummyWritableByteChannel(ucxWritableCh.size)
    }
}
