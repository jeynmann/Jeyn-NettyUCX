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
package io.netty.channel.ucx;

import static io.netty.buffer.PooledByteBufAllocator.MIN_PAGE_SIZE;
import static io.netty.buffer.PooledByteBufAllocator.validateAndCalculatePageShifts;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import io.netty.buffer.*;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UcxPooledByteBufAllocator extends PooledByteBufAllocator {
    public static UcxPooledByteBufAllocator DEFAULT = new UcxPooledByteBufAllocator();
    public static final int MIN_PAGE_SIZE = 4096;
    public static final int DEFAULT_INITIAL_CAPACITY = 256;
    public static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;

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

    @SuppressWarnings({ "unchecked", "deprecation" })
    public UcxPooledByteBufAllocator(int nHeapArena, int nDirectArena, int pageSize,
                                     int maxOrder, int tinyCacheSize, int smallCacheSize,
                                     int normalCacheSize, boolean useCacheForAllThreads) {
        super(true, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize,
              smallCacheSize, normalCacheSize, useCacheForAllThreads);

        assert nDirectArena > 0;

        PoolArena<ByteBuffer>[] directArenas = new PoolArena<ByteBuffer>[nDirectArena];

        int pageShifts = validateAndCalculatePageShifts(pageSize);

        List<PoolArenaMetric> metrics = new ArrayList<PoolArenaMetric>(nDirectArena);
        for (int i = 0; i < nDirectArena; i++) {
            UcxArena arena = new UcxArena(this, pageSize, maxOrder, pageShifts,
                                          chunkSize, directMemoryCacheAlignment);
            directArenas[i] = arena;
            metrics.add(arena);
        }

        Field directArenasField = PooledByteBufAllocator.class.getDeclaredField("directArenas");
        directArenasField.setAccessible(true);
        directArenasField.set(this, directArenas);

        Field directArenaMetricsFiled = PooledByteBufAllocator.class.getDeclaredField("directArenaMetrics");
        directArenaMetricsFiled.setAccessible(true);
        directArenaMetricsFiled.set(this, Collections.unmodifiableList(metrics));
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
}
