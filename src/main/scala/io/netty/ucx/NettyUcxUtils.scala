package io.netty.channel.ucx

import org.openucx.jucx.UcxException

import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.Locale
import java.math.{MathContext, RoundingMode}

import sun.nio.ch.{DirectBuffer, FileChannelImpl}

class NettyUcxId {
    private val buf = ByteBuffer.allocateDirect(UnsafeUtils.LONG_SIZE)
    private var id: Long = 0

    set(address())

    def address(): Long = UnsafeUtils.getAddress(buf)

    def directBuffer(): ByteBuffer = {
        buf.duplicate()
    }

    def get() = id

    def set(i: Long): Unit = {
      buf.putLong(i)
      buf.rewind()
      id = i
    }
}

object NettyUcxUtils {

    def bytesToString(size: Long): String = bytesToString(BigInt(size))

    def bytesToString(size: BigInt): String = {
        val EB = 1L << 60
        val PB = 1L << 50
        val TB = 1L << 40
        val GB = 1L << 30
        val MB = 1L << 20
        val KB = 1L << 10

        if (size >= BigInt(1L << 11) * EB) {
            // The number is too large, show it in scientific notation.
            BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
        } else {
            val (value, unit) = {
                if (size >= 2 * EB) {
                    (BigDecimal(size) / EB, "EB")
                } else if (size >= 2 * PB) {
                    (BigDecimal(size) / PB, "PB")
                } else if (size >= 2 * TB) {
                    (BigDecimal(size) / TB, "TB")
                } else if (size >= 2 * GB) {
                    (BigDecimal(size) / GB, "GB")
                } else if (size >= 2 * MB) {
                    (BigDecimal(size) / MB, "MB")
                } else if (size >= 2 * KB) {
                    (BigDecimal(size) / KB, "KB")
                } else {
                    (BigDecimal(size), "B")
                }
            }
            "%.1f %s".formatLocal(Locale.US, value, unit)
        }
    }
}

object UnsafeUtils {
    val INT_SIZE: Int = 4
    val LONG_SIZE: Int = 8

    private val classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer")
    private val directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(classOf[Long], classOf[Int])
    directBufferConstructor.setAccessible(true)

    def getByteBufferView(address: Long, length: Int): ByteBuffer with DirectBuffer = {
        directBufferConstructor.newInstance(address.asInstanceOf[Object], length.asInstanceOf[Object])
          .asInstanceOf[ByteBuffer with DirectBuffer]
    }

    def getAddress(buffer: ByteBuffer): Long = {
        buffer.asInstanceOf[sun.nio.ch.DirectBuffer].address
    }
}

object MmapUtils extends UcxLogging {
    private val mmap = classOf[FileChannelImpl].getDeclaredMethod("map0", classOf[Int], classOf[Long], classOf[Long])
    mmap.setAccessible(true)

    private val unmmap = classOf[FileChannelImpl].getDeclaredMethod("unmap0", classOf[Long], classOf[Long])
    unmmap.setAccessible(true)

    def mmap(fileChannel: FileChannel, offset: Long, length: Long): Long = {
        try {
            mmap.invoke(fileChannel, 1.asInstanceOf[Object], offset.asInstanceOf[Object], length.asInstanceOf[Object])
                .asInstanceOf[Long]
        } catch {
            case e: Exception =>
                logError(s"Failed to mmap (${fileChannel.size()} $offset $length): $e")
                throw new UcxException(e.getMessage)
        }
    }

    def munmap(address: Long, length: Long): Unit = {
        try {
            unmmap.invoke(null, address.asInstanceOf[Object], length.asInstanceOf[Object])
        } catch {
            case e@(_: IllegalAccessException | _: InvocationTargetException) =>
                logError(e.getMessage)
        }
    }
}

class UcxDebuger extends UcxLogging {
    private var id = 0

    def debugInfo(cls: => Class[_] = this.getClass()) = {
        id += 1
        logInfo(s"$cls stage $id")
    }
}

object UcxDebuger{
    private val inst = new UcxDebuger

    def debugInfo[T](t: T) = inst.debugInfo(t.getClass)
}
