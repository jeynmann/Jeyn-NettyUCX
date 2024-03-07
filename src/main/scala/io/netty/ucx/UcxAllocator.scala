
package io.netty.channel.ucx.memory

import io.netty.channel.ucx.UcxLogging
import io.netty.channel.ucx.NettyUcxUtils
import io.netty.channel.ucx.UnsafeUtils

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.openucx.jucx.ucs.UcsConstants
import java.util.concurrent.Semaphore

class UcxMemory(val address: Long, val size: Long) {
  def close(): Unit = {}
}

class UcxSharedMemory(val closeCb: () => Unit, val refCount: AtomicInteger,
                      override val address: Long, override val size: Long)
  extends UcxMemory(address, size) {

  override def close(): Unit = {
    if (refCount.decrementAndGet() == 0) {
      closeCb()
    }
  }
}

class UcxRegMemory(
  private[ucx] val memory: UcpMemory,
  private[ucx] val allocator: UcxAllocator,
  override val address: Long, override val size: Long)
  extends UcxMemory(address, size) {
  override def close(): Unit = {}
}

class UcxLinkedMemBlock(private[memory] val superMem: UcxLinkedMemBlock,
                        private[memory] var broMem: UcxLinkedMemBlock,
                        private[ucx] override val memory: UcpMemory,
                        private[ucx] override val allocator: UcxAllocator,
                        override val address: Long, override val size: Long)
  extends UcxRegMemory(memory, allocator, address, size) {
}

trait UcxAllocator extends Closeable {
  def allocate(): UcxRegMemory
  def deallocate(mem: UcxRegMemory): Unit
  def preallocate(numBuffers: Int): Unit = {
    (0 until numBuffers).map(x => allocate()).foreach(_.close())
  }
}

class UcxAllocatorStack extends UcxAllocator with UcxLogging {
  private[memory] val stack = new ConcurrentLinkedDeque[UcxRegMemory]
  private[memory] val numAllocs = new AtomicInteger(0)
  private[memory] val memMapParams = new UcpMemMapParams().allocate()
    .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

  override def allocate(): UcxRegMemory = ???
  override def deallocate(mem: UcxRegMemory): Unit = ???
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
}

case class UcxLinkedAllocatorStack(length: Long, minRegistrationSize: Long,
                                   next: UcxLinkedAllocatorStack,
                                   ucxContext: UcpContext)
  extends UcxAllocatorStack() with Closeable {
  private[this] lazy val registrationSize = length.max(minRegistrationSize)
  private[this] var limit: Semaphore = _
  logInfo(s"Allocator stack size $length")
  if (next == null) {
    memMapParams.setLength(registrationSize)
  }

  override def allocate(): UcxRegMemory = {
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
          stack.add(new UcxLinkedMemBlock(null, null, memory, this, address, length))
          address += length
        }
        result = stack.pollFirst()
      }
      result
    } else {
      val superMem = next.allocate().asInstanceOf[UcxLinkedMemBlock]
      val address1 = superMem.address
      val address2 = address1 + length
      val block1 = new UcxLinkedMemBlock(superMem, null, superMem.memory, this,
                                         address1, length)
      val block2 = new UcxLinkedMemBlock(superMem, null, superMem.memory, this,
                                         address2, length)
      block1.broMem = block2
      block2.broMem = block1
      stack.add(block2)
      block1
    }
  }

  override def deallocate(memBlock: UcxRegMemory): Unit = {
    val block = memBlock.asInstanceOf[UcxLinkedMemBlock]
    if ((block.superMem == null) || (!stack.remove(block.broMem))) {
      stack.add(block)
    } else {
      next.deallocate(block.superMem)
    }
    releaseLimit()
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
      var superAllocator: UcxLinkedAllocatorStack = null
      for (j <- 0 until memGroupSize.min(memRange.length - i)) {
        val memSize = memRange(i + j)
        if ((memSize >= minBufferSize) && (memSize <= maxBufferSize)) {
          val current = new UcxLinkedAllocatorStack(memSize, minRegistrationSize,
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

  protected val allocatorMap = new ConcurrentHashMap[Long, UcxAllocator]()

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }

  def get(size: Long): UcxRegMemory = {
    allocatorMap.get(roundUpToTheNextPowerOf2(size)).allocate()
  }
}

// class UcxBuf extends ByteBuf {}

// class UcxRingBuffer(ucxContext: UcpContext, registrationSize: Long)
//   extends Closeable {
//   private[memory] val stack = new ConcurrentLinkedDeque[UcxRegMemory]

//   private[memory] val ucpMemParam = new UcpMemMapParams().allocate()
//     .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
//     .setLength(registrationSize)
//   private[memory] val ucpMem = ucxContext.memoryMap(ucpMemParam)

//   private[memory] val begin = ucpMem.getAddress
//   private[memory] val end = ucpMem.getAddress + ucpMem.getLength

//   private[memory] val usedBegin = new AtomicLong(begin)
//   private[memory] val usedEnd = new AtomicLong(begin)

//   override def allocate(size: Long): UcxRegMemory = {
//   }

//   override def deallocate(mem: UcxRegMemory): Unit = {
    
//   }

//   override def close(): Unit = {
//     if (ucpMem.getNativeId != null) {
//       ucpMem.deregister()
//     }
//   }
// }

// case class UcxRingBufferPool(ucxContext: UcpContext)
//   extends Closeable with UcxLogging {
//   private[memory] val memGroupSize = 3
//   private[memory] val maxMemFactor = 1L - 1L / (1L << (memGroupSize - 1L))
//   private[memory] var minBufferSize: Long = 4096L
//   private[memory] var maxBufferSize: Long = 2L * 1024 * 1024 * 1024
//   private[memory] var minRegistrationSize: Long = 1024L * 1024
//   private[memory] var maxRegistrationSize: Long = 16L * 1024 * 1024 * 1024

//   def init(minBufSize: Long, maxBufSize: Long, minRegSize: Long, maxRegSize: Long, preAllocMap: Map[Long, Int], limit: Boolean):
//     Unit = {
//     minBufferSize = roundUpToTheNextPowerOf2(minBufSize)
//     maxBufferSize = roundUpToTheNextPowerOf2(maxBufSize)
//     minRegistrationSize = roundUpToTheNextPowerOf2(minRegSize)
//     maxRegistrationSize = roundUpToTheNextPowerOf2(maxRegSize * maxMemFactor)

//     val memRange = (1 until 47).map(1L << _).reverse
//     val minLimit = (maxRegistrationSize / maxBufferSize).max(1L)
//                                                         .min(Int.MaxValue)
//     logInfo(s"limit $limit buf ($minBufferSize, $maxBufferSize) reg " +
//             s"($minRegistrationSize, $maxRegistrationSize)")

//     for (i <- 0 until memRange.length by memGroupSize) {
//       var superAllocator: UcxLinkedAllocatorStack = null
//       for (j <- 0 until memGroupSize.min(memRange.length - i)) {
//         val memSize = memRange(i + j)
//         if ((memSize >= minBufferSize) && (memSize <= maxBufferSize)) {
//           val current = new UcxLinkedAllocatorStack(memSize, minRegistrationSize,
//                                                   superAllocator, ucxContext)
//           // set limit to top allocator
//           if (limit && (superAllocator == null)) {
//             val memLimit = (maxRegistrationSize / memSize).min(minLimit << i)
//                                                           .max(1L)
//                                                           .min(Int.MaxValue)
//             logDebug(s"mem $memSize limit $memLimit")
//             current.setLimit(memLimit.toInt)
//           }
//           superAllocator = current
//           allocatorMap.put(memSize, current)
//         }
//       }
//     }
//     preAllocMap.foreach{
//       case (size, count) => {
//         allocatorMap.get(roundUpToTheNextPowerOf2(size)).preallocate(count)
//       }
//     }
//   }

//   protected def roundUpToTheNextPowerOf2(size: Long): Long = {
//     if (size < minBufferSize) {
//       minBufferSize
//     } else {
//       // Round up length to the nearest power of two
//       var length = size
//       length -= 1
//       length |= length >> 1
//       length |= length >> 2
//       length |= length >> 4
//       length |= length >> 8
//       length |= length >> 16
//       length += 1
//       length
//     }
//   }

//   protected val allocatorMap = new ConcurrentHashMap[Long, UcxAllocator]()

//   override def close(): Unit = {
//     allocatorMap.values.forEach(allocator => allocator.close())
//     allocatorMap.clear()
//   }

//   def get(size: Long): UcxRegMemory = {
//     allocatorMap.get(roundUpToTheNextPowerOf2(size)).allocate()
//   }
// }
