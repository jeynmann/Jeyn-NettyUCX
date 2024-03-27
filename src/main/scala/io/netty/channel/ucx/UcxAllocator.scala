
package io.netty.channel.ucx.memory

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}
import java.util.concurrent.Semaphore

import io.netty.channel.ucx.UcxLogging
import io.netty.channel.ucx.NettyUcxUtils
import io.netty.channel.ucx.UnsafeUtils

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.openucx.jucx.ucs.UcsConstants

class UcxMemory(val address: Long, val size: Long) {
  def close(): Unit = {}
}

class UcxPooledMemory(allocator: UcxAllocator, address: Long, size: Long) extends
  UcxMemory(address, size) {
  override def close(): Unit = allocator.deallocate(this)
}

trait UcxAllocator extends Closeable {
  def allocate(length: Long): UcxMemory
  def deallocate(mem: UcxMemory): Unit
}

class UcxPooledAllocator(val poolSize: Long, ucpContext: UcpContext)
  extends UcxAllocator with UcxLogging {
  protected val memMapParams = new UcpMemMapParams().allocate()
    .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    .setLength(poolSize)
  protected val mem = ucpContext.memoryMap(memMapParams)
  protected val address = mem.getAddress

  protected val recycle = new ConcurrentHashMap[Long, Long]
  protected var allocatedFront = 0l
  protected var allocatedEnd = 0l
  protected var availableSize = poolSize - 1l

  override def allocate(length: Long): UcxMemory = {
    if (availableSize < length) {
      return null
    }

    if (allocatedFront == allocatedEnd) {
      assert(allocatedFront == 0l)
      allocatedEnd = length
      availableSize -= length
      return new UcxPooledMemory(this, address, length)
    } else if (allocatedFront < allocatedEnd) {
      if (allocatedEnd + length <= poolSize) {
        val memAddress = address + allocatedEnd
        allocatedEnd = (allocatedEnd + length) % poolSize
        availableSize -= length
        return new UcxPooledMemory(this, memAddress, length)
      }

      if (length < allocatedFront) {
        val padding = poolSize - allocatedEnd
        recycle.putIfAbsent(allocatedEnd, padding)
        allocatedEnd = length
        availableSize -= length + padding
        return new UcxPooledMemory(this, address, length)
      }
    } else {
      if (allocatedEnd + length < allocatedFront) {
        val memAddress = address + allocatedEnd
        allocatedEnd += length
        availableSize -= length
        return new UcxPooledMemory(this, memAddress, length)
      }
    }

    return null
  }

  override def deallocate(mem: UcxMemory): Unit = {
    val memOffset = mem.address - address

    if (allocatedFront == memOffset) {
      allocatedFront = (memOffset + mem.size) % poolSize
      availableSize += mem.size
      replayPending()
    } else {
      recycle.putIfAbsent(memOffset, mem.size)
    }
  }

  def replayPending(): Unit = {
    var front = allocatedFront
    if (recycle.containsKey(front)) {
      do {
        val recycleSize = recycle.get(front)
        availableSize += recycleSize
        front = (front + recycleSize) % poolSize
      } while (recycle.containsKey(front))

      allocatedFront = front
    }
    // reset to begin if empty
    if (allocatedFront == allocatedEnd) {
      allocatedFront = 0l
      allocatedEnd = 0l
    }
  }

  override def close(): Unit = {
    if (mem.getNativeId != null) {
      mem.deregister()
    }
  }
}

class UcxPreregMemory(
  private[ucx] val memory: UcpMemory,
  private[ucx] val allocator: UcxPreregAllocator,
  override val address: Long, override val size: Long)
  extends UcxMemory(address, size) {
  override def close(): Unit = {}
}

trait UcxPreregAllocator extends Closeable {
  def allocate(): UcxPreregMemory
  def deallocate(mem: UcxPreregMemory): Unit
  
  def preallocate(numBuffers: Int): Unit = {
    (0 until numBuffers).map(x => allocate()).foreach(_.close())
  }
}

class UcxOrderedMem(private[memory] val superMem: UcxOrderedMem,
                    private[memory] var broMem: UcxOrderedMem,
                    private[ucx] override val memory: UcpMemory,
                    private[ucx] override val allocator: UcxPreregAllocator,
                    override val address: Long, override val size: Long)
  extends UcxPreregMemory(memory, allocator, address, size) {
}

case class UcxOrderedAllocator(length: Long, minRegistrationSize: Long,
                               next: UcxOrderedAllocator,
                               ucxContext: UcpContext)
  extends UcxPreregAllocator with UcxLogging {
  private[this] lazy val registrationSize = length.max(minRegistrationSize)
  private[this] var limit: Semaphore = _

  private[memory] val stack = new ConcurrentLinkedDeque[UcxPreregMemory]
  private[memory] val numAllocs = new AtomicInteger(0)
  private[memory] val memMapParams = new UcpMemMapParams().allocate()
    .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

  logInfo(s"Allocator stack size $length")
  if (next == null) {
    memMapParams.setLength(registrationSize)
  }

  override def allocate(): UcxPreregMemory = {
    acquireLimit()
    var result = stack.pollFirst()
    if (result != null) {
      result
    } else if (next == null) {
      logTrace(s"Allocating buffer of size $length.")
      while (result == null) {
        numAllocs.incrementAndGet()
        val memory = ucxContext.memoryMap(memMapParams)
        val numBuffers = memory.getLength / length
        var address = memory.getAddress
        for (i <- 0L until numBuffers) {
          stack.add(new UcxOrderedMem(null, null, memory, this, address, length))
          address += length
        }
        result = stack.pollFirst()
      }
      result
    } else {
      val superMem = next.allocate().asInstanceOf[UcxOrderedMem]
      val address1 = superMem.address
      val address2 = address1 + length
      val block1 = new UcxOrderedMem(superMem, null, superMem.memory, this,
                                         address1, length)
      val block2 = new UcxOrderedMem(superMem, null, superMem.memory, this,
                                         address2, length)
      block1.broMem = block2
      block2.broMem = block1
      stack.add(block2)
      block1
    }
  }

  override def deallocate(memBlock: UcxPreregMemory): Unit = {
    val block = memBlock.asInstanceOf[UcxOrderedMem]
    if ((block.superMem == null) || (!stack.remove(block.broMem))) {
      stack.add(block)
    } else {
      next.deallocate(block.superMem)
    }
    releaseLimit()
  }

  override def close(): Unit = {
    var numBuffers = 0
    var length = 0L
    stack.forEach(block => {
      if (block.memory.getNativeId != null) {
        length = block.size
        block.memory.deregister()
      }
      numBuffers += 1
    })
    if (numBuffers != 0) {
      logInfo(s"Closing $numBuffers buffers size $length allocations " +
        s"${numAllocs.get()}. Total ${NettyUcxUtils.bytesToString(length * numBuffers)}")
      stack.clear()
    }
  }

  def setLimit(num: Int): Unit = {
    limit = new Semaphore(num)
  }

  def acquireLimit() = if (limit != null) {
    limit.acquire(1)
  }

  def releaseLimit() = if (limit != null) {
    limit.release(1)
  }
}

case class UcxOrderedMemPool(ucxContext: UcpContext)
  extends Closeable with UcxLogging {
  private[memory] val memGroupSize = 3
  private[memory] val maxMemFactor = 1L - 1L / (1L << (memGroupSize - 1L))
  private[memory] var minBufferSize: Long = 4096L
  private[memory] var maxBufferSize: Long = 2L * 1024 * 1024 * 1024
  private[memory] var minRegistrationSize: Long = 1024L * 1024
  private[memory] var maxRegistrationSize: Long = 16L * 1024 * 1024 * 1024

  protected val allocatorMap = new ConcurrentHashMap[Long, UcxPreregAllocator]()

  def init(minBufSize: Long, maxBufSize: Long, minRegSize: Long, maxRegSize: Long, preAllocMap: Map[Long, Int], limit: Boolean):
    Unit = {
    minBufferSize = roundUpToTheNextPowerOf2(minBufSize)
    maxBufferSize = roundUpToTheNextPowerOf2(maxBufSize)
    minRegistrationSize = roundUpToTheNextPowerOf2(minRegSize)
    maxRegistrationSize = roundUpToTheNextPowerOf2(maxRegSize * maxMemFactor)

    val memRange = (1 until 47).map(1L << _).reverse
    val minLimit = (maxRegistrationSize / maxBufferSize).max(1L)
                                                        .min(Int.MaxValue)
    logInfo(s"limit $limit buf ($minBufferSize, $maxBufferSize) reg " +
            s"($minRegistrationSize, $maxRegistrationSize)")

    for (i <- 0 until memRange.length by memGroupSize) {
      var superAllocator: UcxOrderedAllocator = null
      for (j <- 0 until memGroupSize.min(memRange.length - i)) {
        val memSize = memRange(i + j)
        if ((memSize >= minBufferSize) && (memSize <= maxBufferSize)) {
          val current = new UcxOrderedAllocator(memSize, minRegistrationSize,
                                                  superAllocator, ucxContext)
          // set limit to top allocator
          if (limit && (superAllocator == null)) {
            val memLimit = (maxRegistrationSize / memSize).min(minLimit << i)
                                                          .max(1L)
                                                          .min(Int.MaxValue)
            logDebug(s"mem $memSize limit $memLimit")
            current.setLimit(memLimit.toInt)
          }
          superAllocator = current
          allocatorMap.put(memSize, current)
        }
      }
    }
    preAllocMap.foreach{
      case (size, count) => {
        allocatorMap.get(roundUpToTheNextPowerOf2(size)).preallocate(count)
      }
    }
  }

  protected def roundUpToTheNextPowerOf2(size: Long): Long = {
    if (size < minBufferSize) {
      minBufferSize
    } else {
      // Round up length to the nearest power of two
      var length = size
      length -= 1
      length |= length >> 1
      length |= length >> 2
      length |= length >> 4
      length |= length >> 8
      length |= length >> 16
      length += 1
      length
    }
  }

  def get(size: Long): UcxPreregMemory = {
    allocatorMap.get(roundUpToTheNextPowerOf2(size)).allocate()
  }

  override def close(): Unit = {
    allocatorMap.values.forEach(_.close())
    allocatorMap.clear()
  }
}
