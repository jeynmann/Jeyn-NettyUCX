package io.netty.buffer;

import java.util.function.Consumer;
import java.nio.ByteBuffer;

import io.netty.util.internal.PlatformDependent;

public class UcxUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
    private Consumer<ByteBuffer> freeMemory;

    public UcxUnsafeDirectByteBuf(ByteBufAllocator alloc, long memoryAddress, int size, 
        Consumer<ByteBuffer> freeMemory) {
        super(alloc, PlatformDependent.directBuffer(memoryAddress, size), size, true);
        this.freeMemory = freeMemory;
    }

    @Override
    protected void freeDirect(ByteBuffer buffer) {
        freeMemory.accept(buffer);
    }
}
