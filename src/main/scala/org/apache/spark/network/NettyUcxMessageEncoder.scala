
package org.apache.spark.network.protocol

import java.util.List
import java.nio.channels.WritableByteChannel

import io.netty.buffer.ByteBuf
import io.netty.channel.FileRegion
import io.netty.channel.DefaultFileRegion
import io.netty.util.ReferenceCountUtil
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ucx.UcxWritableByteChannel
import io.netty.channel.ucx.UcxDummyWritableByteChannel
import io.netty.channel.ucx.UcxDefaultFileRegionMsg
import io.netty.channel.ucx.UcxScatterMessage
import io.netty.handler.codec.MessageToMessageEncoder

import org.apache.spark.network.buffer.ManagedBuffer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
class NettyUcxMessageEncoder extends MessageToMessageEncoder[Message] {

  private final val logger = LoggerFactory.getLogger(classOf[NettyUcxMessageEncoder])

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   */
  override
  def encode(ctx: ChannelHandlerContext, in: Message, out: List[Object]): Unit = {
    var body: Object = null
    var bodyLength: Long = 0
    var isBodyInFrame: Boolean = false

    // If the message has a body, take it out to enable zero-copy transfer for the payload.
    if (in.body() != null) {
      try {
        bodyLength = in.body().size()
        body = in.body().convertToNetty()
        isBodyInFrame = in.isBodyInFrame()
      } catch{
        case e: Exception => {
          in.body().release()
          in match {
            case resp: AbstractResponseMessage => {
              // Re-encode this message as a failure response.
              val error = if (e.getMessage() != null) e.getMessage() else "null"
              logger.error(s"Error processing $in for client ${ctx.channel().remoteAddress()}", e)
              encode(ctx, resp.createFailureResponse(error), out)
            }
            case _ => throw e
          }
        }
      }

    }

    val msgType = in.`type`()
    // All messages have the frame length, message type, and message itself. The frame length
    // may optionally include the length of the body data, depending on what message is being
    // sent.
    val headerLength = 8 + msgType.encodedLength() + in.encodedLength()
    val frameLength = headerLength + (if (isBodyInFrame) bodyLength else 0)

    def encodeHeader(header: ByteBuf): Unit = {
      header.writeLong(frameLength)
      msgType.encode(header)
      in.encode(header)
    }

    if (body != null) {
      val ucxMessage = new UcxScatterMessage(
        ctx.channel().asInstanceOf[io.netty.channel.ucx.UcxSocketChannel])
      val header = ctx.alloc().directBuffer(headerLength)
      encodeHeader(header)
      ucxMessage.addByteBuf(header)
      body match {
        case fr: DefaultFileRegion => ucxMessage.addDefaultFileRegion(fr)
        case buf: ByteBuf => ucxMessage.addByteBuf(buf)
        case fr: FileRegion => ucxMessage.addFileRegion(fr)
      }
      out.add(ucxMessage)
    } else {
      val header = ctx.alloc().directBuffer(headerLength)
      encodeHeader(header)
      out.add(header)
    }
  }

}

object NettyUcxMessageEncoder {
  final val INSTANCE = new NettyUcxMessageEncoder()
}

class UcxMessageWithHeader(managedBuffer: ManagedBuffer, header: ByteBuf,
                           body: Object, bodyLength: Long)
  extends MessageWithHeader(managedBuffer, header, body, bodyLength) {
  protected val headerLength = header.readableBytes()
  protected var totalBytesTransferred: Long = 0L
  protected var bodyTransferred: Long = 0L

  // private final val logger = LoggerFactory.getLogger(classOf[NettyUcxMessageEncoder])

  override def transferred(): Long = {
    return totalBytesTransferred
  }

  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    assert(position == totalBytesTransferred, "Invalid position.")
    // Bytes written for header in this call.
    var writtenHeader: Long = 0
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target)
      totalBytesTransferred += writtenHeader
      if (header.readableBytes() > 0) {
        return writtenHeader
      }
    }

    // Bytes written for body in this call.
    var writtenBody: Long = 0
    body match {
      case fr: DefaultFileRegion =>
        writtenBody = copyDefaultFileRegion(fr, target)
      case buf: ByteBuf =>
        writtenBody = copyByteBuf(buf, target)
      case fr: FileRegion =>
        writtenBody = fr.transferTo(target, totalBytesTransferred - headerLength)
      case _ => throw new IllegalArgumentException(s"unsupported type: $body")
    }

    totalBytesTransferred += writtenBody
    bodyTransferred += writtenBody

    return writtenHeader + writtenBody
  }

  override protected def deallocate(): Unit = {
    header.release()
    ReferenceCountUtil.release(body)
    if (managedBuffer != null) {
      managedBuffer.release()
    }
  }

  protected def copyDefaultFileRegion(fr: DefaultFileRegion,
                                      target: WritableByteChannel): Long = {
    val offset = fr.position() + fr.transferred() + bodyTransferred

    target match {
      case ucxCh: UcxWritableByteChannel => {
        val fileCh = UcxDefaultFileRegionMsg.getChannel(fr)
        val byteBuf = ucxCh.internalByteBuf()
        val length = byteBuf.writableBytes()
        UcxDefaultFileRegionMsg.readDefaultFileRegion(fileCh, offset, length, byteBuf)

        // logger.info(s"fr ${fr.position} ${fr.transferred}")
        // move forward the flags of DefaultFileRegion
        return length
      }
      case ch: WritableByteChannel => {
        return fr.transferTo(ch, fr.transferred())
      }
    }
  }

  protected def copyByteBuf(buf: ByteBuf, target: WritableByteChannel): Long = {
    // SPARK-24578: cap the sub-region's size of returned nio buffer to improve the performance
    // for the case that the passed-in buffer has too many components.
    val length = buf.readableBytes()
    // If the ByteBuf holds more then one ByteBuffer we should better call nioBuffers(...)
    // to eliminate extra memory copies.
    var written = 0
    if (buf.nioBufferCount() == 1) {
      val buffer = buf.nioBuffer(buf.readerIndex(), length)
      written = target.write(buffer)
    } else {
      val buffers = buf.nioBuffers(buf.readerIndex(), length)
      for (buffer <- buffers) {
        written += target.write(buffer)
      }
    }
    buf.skipBytes(written)
    return written
  }
}