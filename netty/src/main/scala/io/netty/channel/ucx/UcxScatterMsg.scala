package io.netty.channel.ucx

import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.UcxUnsafeDirectByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.Unpooled
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

    def toDirectByteBuf(fr: UcxFileRegion, fc: FileChannel, offset: Long,
                        length: Long, alloc: ByteBufAllocator): ByteBuf = {
        val directBuf = alloc.directBuffer(length.toInt, length.toInt)

        directBuf.writeBytes(fc, offset, length.toInt)
        fr.release()
        directBuf
    }

    def toMapedByteBuf(fr: UcxFileRegion, mmapPtr: Long, offset: Long, length: Long,
                       alloc: ByteBufAllocator): ByteBuf = {
        new UcxUnsafeDirectByteBuf(alloc, mmapPtr + offset, length.toInt, _ => fr.release())
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

class UcxDefaultFileRegionFrame(
    fr: DefaultFileRegion, fc: FileChannel, offset: Long, length: Long,
    alloc: ByteBufAllocator) extends UcxMsgFrame(fr) {
    // file could be very large, use lazy load here
    override
    def convertToByteBuf(): ByteBuf = {
        val offset = this.offset + fr.position() 
        UcxConverter.toDirectByteBuf(fr, fc, offset, length, alloc)
    }
}

class UcxUcxFileRegionFrame(
    fr: UcxFileRegion, offset: Long, length: Long,
    alloc: ByteBufAllocator) extends UcxMsgFrame(fr) {
    // file could be very large, use lazy load here
    override
    def convertToByteBuf(): ByteBuf = {
        if (fr.canMmap()) {
            UcxConverter.toMapedByteBuf(fr, fr.getMmap(), offset, length, alloc)
        } else {
            val offset = this.offset + fr.position() 
            UcxConverter.toDirectByteBuf(fr, fr.getChannel(), offset, length, alloc)
        }
    }
}

trait UcxScatterMsg extends AbstractReferenceCounted with UcxLogging {

    protected val frames = new java.util.ArrayList[UcxMsgFrame]()
    protected var readerIndex = 0
    protected var writerIndex = 0

    protected var alloc: ByteBufAllocator = _
    protected var frameSize: Int = _
    protected var spinCount: Int = _
    protected var streamId: Int = _

    protected def init(ucxChannel: UcxSocketChannel) = {
        val config = ucxChannel.config()
        alloc = config.getAllocator()
        frameSize = config.getFileFrameSize()
        spinCount = config.getWriteSpinCount()
        streamId = UcxScatterMsg.nextId
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

            processor.accept(buf, (streamId, writerIndex, readerIndex))
            readerIndex += 1
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
    type MessageId = (Int, Int, Int)
    type Processor = java.util.function.BiConsumer[ByteBuf, MessageId]

    private val seed = new java.util.Random
    private val streamId = new java.util.concurrent.atomic.AtomicInteger(seed.nextInt)

    def nextId = streamId.incrementAndGet()
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
        do {
            val lengthNow = (limit - offsetNow).toInt.min(frameSize)
            val frame = new UcxFileRegionFrame(fr, offsetNow, lengthNow, alloc)
            frames.add(frame)
            offsetNow += lengthNow
        } while (offsetNow != limit)

        fr.retain(count - 1)
        writerIndex += count
    }
}

trait UcxDefaultFileRegionMsg extends UcxScatterMsg {

    def addDefaultFileRegion2(fr: DefaultFileRegion): Unit = {
        val offset = fr.position()
        val length = fr.count() - fr.transferred()
        if (length == 0) {
            return
        }

        val fc = UcxFileRegion.getChannel(fr)
        if (length <= frameSize) {
            val frame = new UcxDefaultFileRegionFrame(fr, fc, offset, length, alloc)
            frames.add(frame)
            writerIndex += 1
            return
        }

        val count = ((length - 1) / frameSize).toInt + 1
        ensureCapacity(writerIndex + count)

        val limit = offset + length
        var offsetNow = offset
        do {
            val lengthNow = (limit - offsetNow).toInt.min(frameSize)
            val frame = new UcxDefaultFileRegionFrame(fr, fc, offsetNow, lengthNow, alloc)
            frames.add(frame)
            offsetNow += lengthNow
        } while (offsetNow != limit)
        logDev(s"write $frames-$count $offset-$offsetNow-$limit")

        fr.retain(count - 1)
        writerIndex += count
    }

    def addDefaultFileRegion(region: DefaultFileRegion): Unit = {
        val fr = new UcxFileRegion(region)
        val offset = fr.transferred()
        val length = fr.count() - fr.transferred()
        if (length == 0) {
            return
        }

        if (length <= frameSize) {
            val frame = new UcxUcxFileRegionFrame(fr, offset, length, alloc)
            frames.add(frame)
            writerIndex += 1
            return
        }

        val count = ((length - 1) / frameSize).toInt + 1
        ensureCapacity(writerIndex + count)

        val limit = offset + length
        var offsetNow = offset
        do {
            val lengthNow = (limit - offsetNow).toInt.min(frameSize)
            val frame = new UcxUcxFileRegionFrame(fr, offsetNow, lengthNow, alloc)
            frames.add(frame)
            offsetNow += lengthNow
        } while (offsetNow != limit)
        logDev(s"write $frames-$count $offset-$offsetNow-$limit")

        fr.retain(count - 1)
        writerIndex += count
    }
}

trait UcxCompositeByteBufMsg extends UcxScatterMsg {

    def addCompositeByteBuf(buf: CompositeByteBuf): Unit = {
        val offset = buf.readerIndex()
        val length = buf.readableBytes()
        if (length == 0) {
            return
        }

        val bufs = buf.decompose(offset, length)
        val count = bufs.size()

        bufs.forEach(b => {
            val frame = new UcxByteBufFrame(buf, alloc)
            frames.add(frame)
        })
        buf.clear().release()

        writerIndex += count
    }
}

class UcxCompositeByteBufMessage(buf: CompositeByteBuf, ucxChannel: UcxSocketChannel)
    extends UcxCompositeByteBufMsg {

    init(ucxChannel)
    addCompositeByteBuf(buf)

    override
    def addMessage(msg: ReferenceCounted): Unit = {
        msg match {
            case buf: CompositeByteBuf => addCompositeByteBuf(buf)
            case obj => throw new IllegalArgumentException(s"unsupported: $obj")
        }
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

class UcxScatterMessage(ucxChannel: UcxSocketChannel) extends UcxFileRegionMsg
    with UcxDefaultFileRegionMsg with UcxCompositeByteBufMsg {

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
            case fr: CompositeByteBuf => addCompositeByteBuf(fr)
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
