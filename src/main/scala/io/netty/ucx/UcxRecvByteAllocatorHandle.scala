package io.netty.channel.ucx

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufUtil
import io.netty.channel.RecvByteBufAllocator.DelegatingHandle
import io.netty.channel.RecvByteBufAllocator.ExtendedHandle
import io.netty.util.UncheckedBooleanSupplier

private[ucx] class PreferredDirectByteBufAllocator extends ByteBufAllocator {
    private var allocator: ByteBufAllocator = _

    def updateAllocator(a: ByteBufAllocator): Unit = {
        allocator = a;
    }

    override def buffer(): ByteBuf = {
        return allocator.directBuffer();
    }

    override def buffer(initialCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity);
    }

    override def buffer(initialCapacity: Int, maxCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity, maxCapacity);
    }

    override def ioBuffer(): ByteBuf = {
        return allocator.directBuffer();
    }

    override def ioBuffer(initialCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity);
    }

    override def ioBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity, maxCapacity);
    }

    override def heapBuffer(): ByteBuf = {
        return allocator.heapBuffer();
    }

    override def heapBuffer(initialCapacity: Int): ByteBuf = {
        return allocator.heapBuffer(initialCapacity);
    }

    override def heapBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf = {
        return allocator.heapBuffer(initialCapacity, maxCapacity);
    }

    override def directBuffer(): ByteBuf = {
        return allocator.directBuffer();
    }

    override def directBuffer(initialCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity);
    }

    override def directBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf = {
        return allocator.directBuffer(initialCapacity, maxCapacity);
    }

    override def compositeBuffer(): CompositeByteBuf = {
        return allocator.compositeDirectBuffer();
    }

    override def compositeBuffer(maxNumComponents: Int): CompositeByteBuf = {
        return allocator.compositeDirectBuffer(maxNumComponents);
    }

    override def compositeHeapBuffer(): CompositeByteBuf = {
        return allocator.compositeHeapBuffer();
    }

    override def compositeHeapBuffer(maxNumComponents: Int): CompositeByteBuf = {
        return allocator.compositeHeapBuffer(maxNumComponents);
    }

    override def compositeDirectBuffer(): CompositeByteBuf = {
        return allocator.compositeDirectBuffer();
    }

    override def compositeDirectBuffer(maxNumComponents: Int): CompositeByteBuf = {
        return allocator.compositeDirectBuffer(maxNumComponents);
    }

    override def isDirectBufferPooled(): Boolean = {
        return allocator.isDirectBufferPooled();
    }

    override def calculateNewCapacity(minNewCapacity: Int, maxCapacity: Int): Int = {
        return allocator.calculateNewCapacity(minNewCapacity, maxCapacity);
    }
}

private[ucx] object PreferredDirectByteBufAllocator {
    def directBuffer0(allocator: ByteBufAllocator, capacity: Int): ByteBuf = {
        if (capacity == 0) {
            return Unpooled.EMPTY_BUFFER
        }

        if (allocator.isDirectBufferPooled()) {
            return allocator.directBuffer(capacity)
        }

        val threadLocalBuf = ByteBufUtil.threadLocalDirectBuffer()
        if (threadLocalBuf != null) {
            return threadLocalBuf
        }

        return allocator.directBuffer(capacity)
    }
}

class UcxRecvByteAllocatorHandle(handle: ExtendedHandle)
    extends DelegatingHandle(handle) with ExtendedHandle {
    private final val preferredDirectByteBufAllocator = new PreferredDirectByteBufAllocator()
    private final val defaultMaybeMoreDataSupplier = new UncheckedBooleanSupplier() {
        override
        def get(): Boolean = {
            return lastBytesRead() > 0
        }
    }

    override
    final def allocate(alloc: ByteBufAllocator): ByteBuf = {
        // We need to ensure we always allocate a direct ByteBuf as we can only use a direct buffer to read via JNI.
        preferredDirectByteBufAllocator.updateAllocator(alloc)
        return handle.allocate(preferredDirectByteBufAllocator)
    }

    override
    final def continueReading(maybeMoreDataSupplier: UncheckedBooleanSupplier): Boolean = {
        return handle.continueReading(maybeMoreDataSupplier)
    }

    override
    final def continueReading(): Boolean = {
        // TODO: maybe more data to read
        return continueReading(defaultMaybeMoreDataSupplier)
    }
}
