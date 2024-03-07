package io.netty.channel.ucx

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.channel.RecvByteBufAllocator.DelegatingHandle
import io.netty.channel.RecvByteBufAllocator.ExtendedHandle
import io.netty.channel.unix.PreferredDirectByteBufAllocator
import io.netty.util.UncheckedBooleanSupplier

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
        return extendedHandle.allocate(preferredDirectByteBufAllocator)
    }

    override
    final def continueReading(maybeMoreDataSupplier: UncheckedBooleanSupplier): Boolean = {
        return extendedHandle.continueReading(maybeMoreDataSupplier)
    }

    override
    final def continueReading(): Boolean = {
        // TODO: maybe more data to read
        return continueReading(defaultMaybeMoreDataSupplier)
    }

    def extendedHandle = delegate().asInstanceOf[ExtendedHandle]
}
