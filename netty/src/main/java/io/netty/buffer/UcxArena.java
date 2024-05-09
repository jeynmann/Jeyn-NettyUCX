
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ConcurrentHashMap;

import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpMemory;

public class UcxArena extends PoolArena<ByteBuffer> {
    UcxArena(UcpContext ucpContext, PooledByteBufAllocator parent, int pageSize,
             int maxOrder, int pageShifts, int chunkSize, int alignment) {
        super(parent, pageSize, maxOrder, pageShifts, chunkSize, alignment);
        this.ucpContext = ucpContext;
        this.regChunks = new ConcurrentHashMap<>();
        this.unpooledCache = new AtomicReference<>();
    }

    UcpContext ucpContext;
    ConcurrentHashMap<Long, UcpMemory> regChunks;
    AtomicReference<PoolChunk<ByteBuffer>> unpooledCache;

    @Override
    boolean isDirect() {
        return true;
    }

    protected ByteBuffer toRigisteredBuffer(ByteBuffer buf) {
        regChunks.computeIfAbsent(UcxUtils.getAddress(buf),
                                  (Long address) -> ucpContext.registerMemory(buf));
        return buf;
    }

    protected ByteBuffer toDerigisterBuffer(ByteBuffer buf) {
        UcpMemory ucpMemory = regChunks.remove(UcxUtils.getAddress(buf));
        if (ucpMemory != null) {
            ucpMemory.deregister();
        }
        return buf;
    }

    // mark as package-private, only for unit test
    int offsetCacheLine(ByteBuffer memory) {
        // We can only calculate the offset if Unsafe is present as otherwise directBufferAddress(...) will
        // throw an NPE.
        int remainder = HAS_UNSAFE ?
                        (int) (PlatformDependent.directBufferAddress(memory) &
                               directMemoryCacheAlignmentMask): 0;

        // offset = alignment - address & (alignment - 1)
        return directMemoryCacheAlignment - remainder;
    }

    @Override
    protected PoolChunk<ByteBuffer> newChunk(int pageSize, int maxOrder,
                                             int pageShifts, int chunkSize) {
        if (directMemoryCacheAlignment == 0) {
            final ByteBuffer memory = allocateDirect(chunkSize);
            return new PoolChunk<ByteBuffer>(this, toRigisteredBuffer(memory),
                                             pageSize, maxOrder, pageShifts,
                                             chunkSize, 0);
        }
        final ByteBuffer memory = allocateDirect(chunkSize +
                                                 directMemoryCacheAlignment);
        return new PoolChunk<ByteBuffer>(this, toRigisteredBuffer(memory), pageSize, maxOrder,
                                         pageShifts, chunkSize,
                                         offsetCacheLine(memory));
    }

    @Override
    protected PoolChunk<ByteBuffer> newUnpooledChunk(int capacity) {
        final PoolChunk<ByteBuffer> memoryCache = unpooledCache.get();
        if (memoryCache != null &&
            unpooledCache.compareAndSet(memoryCache, null)) {
            if (capacity <= memoryCache.chunkSize()) {
                return memoryCache;
            }

            destroyChunkNoCache(memoryCache);
        }

        return newUnpooledChunkNoCache(capacity);
    }

    protected PoolChunk<ByteBuffer> newUnpooledChunkNoCache(int capacity) {
        if (directMemoryCacheAlignment == 0) {
            final ByteBuffer memory = allocateDirect(capacity);
            return new PoolChunk<ByteBuffer>(this, toRigisteredBuffer(memory),
                                             capacity, 0);
        }
        final ByteBuffer memory = allocateDirect(capacity +
                                                 directMemoryCacheAlignment);
        return new PoolChunk<ByteBuffer>(this, toRigisteredBuffer(memory),
                                         capacity, offsetCacheLine(memory));
    }

    private static ByteBuffer allocateDirect(int capacity) {
        return PlatformDependent.useDirectBufferNoCleaner() ?
               PlatformDependent.allocateDirectNoCleaner(capacity) :
               ByteBuffer.allocateDirect(capacity);
    }

    @Override
    protected void destroyChunk(PoolChunk<ByteBuffer> chunk) {
        if (unpooledCache.compareAndSet(null, chunk)) {
            return;
        }

        final PoolChunk<ByteBuffer> memoryCache = unpooledCache.get();
        if (memoryCache.chunkSize() < chunk.chunkSize() &&
            unpooledCache.compareAndSet(memoryCache, chunk)) {
            destroyChunkNoCache(memoryCache);
            return;
        }

        destroyChunkNoCache(chunk);
    }

    protected void destroyChunkNoCache(PoolChunk<ByteBuffer> chunk) {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            PlatformDependent.freeDirectNoCleaner(
                toDerigisterBuffer(chunk.memory));
        } else {
            PlatformDependent.freeDirectBuffer(
                toDerigisterBuffer(chunk.memory));
        }
    }

    @Override
    protected PooledByteBuf<ByteBuffer> newByteBuf(int maxCapacity) {
        if (HAS_UNSAFE) {
            return PooledUnsafeDirectByteBuf.newInstance(maxCapacity);
        } else {
            return PooledDirectByteBuf.newInstance(maxCapacity);
        }
    }

    @Override
    protected void memoryCopy(ByteBuffer src, int srcOffset,
                              PooledByteBuf<ByteBuffer> dstBuf, int length) {
        if (length == 0) {
            return;
        }

        if (HAS_UNSAFE) {
            PlatformDependent.copyMemory(
                PlatformDependent.directBufferAddress(src) + srcOffset,
                PlatformDependent.directBufferAddress(dstBuf.memory) + dstBuf.offset,
                length);
        } else {
            // We must duplicate the NIO buffers because they may be accessed by other Netty buffers.
            src = src.duplicate();
            ByteBuffer dst = dstBuf.internalNioBuffer();
            src.position(srcOffset).limit(srcOffset + length);
            dst.position(dstBuf.offset);
            dst.put(src);
        }
    }
}
