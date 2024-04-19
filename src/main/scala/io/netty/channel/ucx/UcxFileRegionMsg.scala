package io.netty.channel.ucx

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion
import io.netty.util.ReferenceCounted
import io.netty.util.AbstractReferenceCounted

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.WritableByteChannel

// @Note The convertion will release refCounter if is a refCounter type
object UcxConverter {
    def toDirectByteBuf(buf: ByteBuf, alloc: ByteBufAllocator): ByteBuf = {
        if (buf.isDirect() || buf.hasMemoryAddress() ||
            (buf.readableBytes() == 0)) {
            return buf
        }

        val readableBytes = buf.readableBytes()
        val directBuf = alloc.directBuffer(readableBytes, readableBytes)

        directBuf.writeBytes(buf, buf.readerIndex(), readableBytes)
        buf.release()
        return directBuf
    }

    def toDirectByteBuf(fr: FileRegion, offset: Long, length: Long,
                        alloc: ByteBufAllocator): ByteBuf = {
        val byteChannel = new UcxWritableByteChannel(alloc, length.toInt)

        fr.transferTo(byteChannel, offset)
        fr.release()
        byteChannel.internalByteBuf()
    }

    def toDirectByteBuf(fr: DefaultFileRegion, fc: FileChannel, offset: Long,
                        length: Long, alloc: ByteBufAllocator): ByteBuf = {
        val directBuf = alloc.directBuffer(length.toInt, length.toInt)

        directBuf.writeBytes(fc, offset, length.toInt)
        fr.release()
        directBuf
    }
}

abstract class UcxMsgFrame(msg: ReferenceCounted) extends ReferenceCounted{
    // @Note The convertion will release refCounter if converted to another object
    def convertToByteBuf(): ByteBuf

    override
    def retain(): this.type = {
        msg.retain()
        this
    }

    override
    def retain(increment: Int): this.type = {
        msg.retain(increment)
        this
    }

    override
    def release(): Boolean = {
        msg.release()
    }

    override
    def release(decrement: Int): Boolean = {
        msg.release(decrement)
    }

    override
    def refCnt(): Int = {
        msg.refCnt()
    }

    override
    def touch() = this

    override
    def touch(hint: Object) = this
}

class UcxByteBufFrame(buf: ByteBuf, alloc: ByteBufAllocator)
    extends UcxMsgFrame(buf) {
    // convert immediately
    protected val directBuf: ByteBuf = UcxConverter.toDirectByteBuf(buf, alloc)

    override
    def convertToByteBuf(): ByteBuf = directBuf
}

class UcxFileRegionFrame(fr: FileRegion, offset: Long, length: Long,
                         alloc: ByteBufAllocator) extends UcxMsgFrame(fr) {
    // file could be very large, use lazy load here
    override
    def convertToByteBuf(): ByteBuf = {
        UcxConverter.toDirectByteBuf(fr, offset, length, alloc)
    }
}

class UcxDefaultFileRegionFrame(fr: DefaultFileRegion, offset: Long, length: Long,
                                alloc: ByteBufAllocator) extends UcxMsgFrame(fr) {
    protected val fc: FileChannel = UcxDefaultFileRegionMsg.getChannel(fr)
    // file could be very large, use lazy load here
    override
    def convertToByteBuf(): ByteBuf = {
        UcxConverter.toDirectByteBuf(fr, fc, offset, length, alloc)
    }
}

trait UcxScatterMsg extends AbstractReferenceCounted with UcxLogging {

    protected val frames = new java.util.ArrayList[UcxMsgFrame]()
    protected var readerIndex = 0
    protected var writerIndex = 0

    protected var alloc: ByteBufAllocator = _
    protected var frameSize: Int = _
    protected var spinCount: Int = _

    protected def init(ucxChannel: UcxSocketChannel) = {
        val config = ucxChannel.config()
        alloc = config.getAllocator()
        frameSize = config.getFileFrameSize()
        spinCount = config.getWriteSpinCount()
    }

    def ensureCapacity(minCapacity: Int) = frames.ensureCapacity(minCapacity)

    def capacity: Int = frames.size()

    def position(): Int = readerIndex

    def limit(): Int = writerIndex

    def isEmpty(): Boolean = readerIndex == writerIndex

    def remaining(): Int = writerIndex - readerIndex

    def forEachMsg(processor: UcxScatterMsg.Processor): Int = {
        val readLimit = (readerIndex + spinCount).min(writerIndex)
        val readCount = readLimit - readerIndex

        while (readerIndex != readLimit) {
            val buf = frames.get(readerIndex).convertToByteBuf()

            readerIndex += 1
            processor.accept(buf, readerIndex == writerIndex)
        }
        return readCount
    }

    def addMessage(msg: ReferenceCounted): Unit

    override
    def touch() = this

    override
    def touch(hint: Object) = this

    override
    def deallocate(): Unit = {
        if (!isEmpty()) {
            val iter = frames.listIterator(readerIndex)
            while (iter.hasNext()) {
                iter.next().release()
            }
        }
        frames.clear()
    }
}

object UcxScatterMsg {
    type Processor = java.util.function.BiConsumer[ByteBuf, Boolean]
}

trait UcxFileRegionMsg extends UcxScatterMsg {

    def addFileRegion(fr: FileRegion): Unit = {
        val offset = fr.transferred()
        val length = fr.count() - fr.transferred()
        if (length == 0) {
            return
        }

        if (length <= frameSize) {
            val frame = new UcxFileRegionFrame(fr, offset, length, alloc)
            frames.add(frame)
            writerIndex += 1
            return
        }

        val count = ((length - 1) / frameSize).toInt + 1
        ensureCapacity(writerIndex + count)

        val limit = offset + length
        var offsetNow = offset
        while (offsetNow != limit) {
            val lengthNow = (limit - offsetNow).toInt.min(frameSize)
            val frame = new UcxFileRegionFrame(fr, offsetNow, lengthNow, alloc)
            frames.add(frame)
            offsetNow += lengthNow
        }

        fr.retain(count - 1)
        writerIndex += count
    }
}

trait UcxDefaultFileRegionMsg extends UcxScatterMsg {

    def addDefaultFileRegion(fr: DefaultFileRegion): Unit = {
        val offset = fr.position() + fr.transferred()
        val length = fr.count() - fr.transferred()
        if (length == 0) {
            return
        }

        if (length <= frameSize) {
            val frame = new UcxDefaultFileRegionFrame(fr, offset, length, alloc)
            frames.add(frame)
            writerIndex += 1
            return
        }

        val count = ((length - 1) / frameSize).toInt + 1
        ensureCapacity(writerIndex + count)

        val limit = offset + length
        var offsetNow = offset
        while (offsetNow != limit) {
            val lengthNow = (limit - offsetNow).toInt.min(frameSize)
            val frame = new UcxDefaultFileRegionFrame(fr, offsetNow, lengthNow, alloc)
            frames.add(frame)
            offsetNow += lengthNow
        }
        logDev(s"write $frames-$count $offset-$offsetNow-$length")

        fr.retain(count - 1)
        writerIndex += count
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

class UcxFileRegionMessage(fr: FileRegion, ucxChannel: UcxSocketChannel)
    extends UcxFileRegionMsg {

    init(ucxChannel)
    addFileRegion(fr)

    override
    def addMessage(msg: ReferenceCounted): Unit = {
        msg match {
            case fr: FileRegion => addFileRegion(fr)
            case obj => throw new IllegalArgumentException(s"unsupported: $obj")
        }
    }
}

class UcxDefaultFileRegionMessage(fr: DefaultFileRegion, ucxChannel: UcxSocketChannel)
    extends UcxDefaultFileRegionMsg {

    init(ucxChannel)
    addDefaultFileRegion(fr)

    override
    def addMessage(msg: ReferenceCounted): Unit = {
        msg match {
            case fr: DefaultFileRegion => addDefaultFileRegion(fr)
            case obj => throw new IllegalArgumentException(s"unsupported: $obj")
        }
    }
}

class UcxScatterMessage(ucxChannel: UcxSocketChannel)
    extends UcxFileRegionMsg with UcxDefaultFileRegionMsg {

    init(ucxChannel)

    def addByteBuf(buf: ByteBuf): Unit = {
        frames.add(new UcxByteBufFrame(buf, alloc))
        writerIndex += 1
    }

    override
    def addMessage(msg: ReferenceCounted): Unit = {
        msg match {
            case fr: DefaultFileRegion => addDefaultFileRegion(fr)
            case fr: FileRegion => addFileRegion(fr)
            case fr: ByteBuf => addByteBuf(fr)
            case obj => throw new IllegalArgumentException(s"unsupported: $obj")
        }
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
