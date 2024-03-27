/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.openucx.jucx.NativeLibs;
import org.openucx.jucx.ucp.UcpParams;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.openucx.jucx.ucp.UcpContext;

public class UcxPooledByteBufAllocator extends PooledByteBufAllocator {
    public static UcxPooledByteBufAllocator DEFAULT = new UcxPooledByteBufAllocator();
    
    public static final int MIN_PAGE_SIZE = 4096;
    public static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) >> 1);
    public static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT = MIN_PAGE_SIZE;
    public static final int DEFAULT_INITIAL_CAPACITY = 256;
    public static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
    
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(
        PooledByteBufAllocator.class);
    private static boolean loadNative = false;
    private static Throwable loadError = null;

    static {
        try {
            NativeLibs.load();
            loadNative = true;
        } catch (Throwable e) {
            logger.error("load NativeLibs:", e);
            loadError = e;
        }
    }

    // TODO: estimate eps
    public static final UcpParams UCP_PARAMS = new UcpParams()
            .requestAmFeature().requestWakeupFeature().setMtWorkersShared(true)
            .setConfig("USE_MT_MUTEX", "yes").setEstimatedNumEps(4000);
    public static final UcpContext UCP_CONTEXT = new UcpContext(UCP_PARAMS);

    @SuppressWarnings("deprecation")
    public UcxPooledByteBufAllocator() {
        this(defaultNumHeapArena(), defaultNumDirectArena(), defaultPageSize(),
             defaultMaxOrder());
    }

    /**
     * @deprecated use
     *             {@link UcxPooledByteBufAllocator#UcxPooledByteBufAllocator(int, int, int, int, int, int, int, boolean)}
     */
    @Deprecated
    public UcxPooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize,
                                     int maxOrder) {
        this(nHeapArena, nDirectArena, pageSize, maxOrder, defaultTinyCacheSize(),
             defaultSmallCacheSize(), defaultNormalCacheSize());
    }

    /**
     * @deprecated use
     *             {@link UcxPooledByteBufAllocator#UcxPooledByteBufAllocator(int, int, int, int, int, int, int, boolean)}
     */
    @Deprecated
    public UcxPooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize,
                                     int maxOrder, int tinyCacheSize, int smallCacheSize,
                                     int normalCacheSize) {
        this(nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize, smallCacheSize,
             normalCacheSize, defaultUseCacheForAllThreads());
    }

    public UcxPooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize,
                                     int maxOrder, int tinyCacheSize, int smallCacheSize,
                                     int normalCacheSize, boolean useCacheForAllThreads) {
        super(true, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize,
              smallCacheSize, normalCacheSize, useCacheForAllThreads,
              DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);

        assert nDirectArena > 0;

        @SuppressWarnings("unchecked")
        PoolArena<ByteBuffer>[] directArenas = new PoolArena[nDirectArena];

        int pageShifts = validateAndCalculatePageShifts(pageSize);
        int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(nDirectArena);
        for (int i = 0; i < nDirectArena; i++) {
            UcxArena arena = new UcxArena(UCP_CONTEXT, this, pageSize, maxOrder,
                                          pageShifts, chunkSize,
                                          DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);
            directArenas[i] = arena;
            metrics.add(arena);
        }

        try {
            Field directArenasField = PooledByteBufAllocator.class.getDeclaredField("directArenas");
            directArenasField.setAccessible(true);
            directArenasField.set(this, directArenas);
        } catch(NoSuchFieldException e) {
            logger.error("get directArenas:", e);
        } catch (IllegalAccessException e) {
            logger.error("set directArenas:", e);
        }

        try {
            Field directArenaMetricsFiled = PooledByteBufAllocator.class.getDeclaredField("directArenaMetrics");
            directArenaMetricsFiled.setAccessible(true);
            directArenaMetricsFiled.set(this, Collections.unmodifiableList(metrics));
        } catch(NoSuchFieldException e) {
            logger.error("get directArenaMetrics:", e);
        } catch (IllegalAccessException e) {
            logger.error("set directArenaMetrics:", e);
        }
    }

    @Override
    public ByteBuf buffer() {
        return directBuffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return compositeDirectBuffer(maxNumComponents);
    }

    public static ByteBuf directBuffer(ByteBufAllocator allocator) {
        return directBuffer(allocator, DEFAULT_INITIAL_CAPACITY,
                            DEFAULT_MAX_CAPACITY);
    }

    public static ByteBuf directBuffer(ByteBufAllocator allocator,
                                       int initialCapacity) {
        return directBuffer(allocator, initialCapacity,
                            DEFAULT_MAX_CAPACITY);
    }

    public static ByteBuf directBuffer(ByteBufAllocator allocator,
                                       int initialCapacity, int maxCapacity) {
        if (allocator instanceof UcxPooledByteBufAllocator) {
            return allocator.directBuffer(initialCapacity, maxCapacity);
        }

        return DEFAULT.directBuffer(initialCapacity, maxCapacity);
    }

    static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = 0; i != maxOrder; i++) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    public static boolean isNativeLoad() {
        return loadNative;
    }

    public static Throwable getNativeError() {
        return loadError;
    }
}
