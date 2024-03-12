

package io.netty.channel.ucx

import org.openucx.jucx.UcxException
import io.netty.channel.ChannelOutboundBuffer

class UcxIOBufException(val buf: ChannelOutboundBuffer, val status: Int, message: String)
extends UcxException(message, status) {}
