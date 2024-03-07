package io.netty.channel.ucx

import io.netty.channel.DefaultChannelConfig
import io.netty.channel.WriteBufferWaterMark
import io.netty.channel.MessageSizeEstimator
import io.netty.channel.RecvByteBufAllocator
import io.netty.channel.socket.ServerSocketChannelConfig
import io.netty.channel.socket.SocketChannelConfig

import io.netty.buffer.ByteBufAllocator

class UcxChannelConfig(channel: AbstractUcxChannel)
    extends DefaultChannelConfig(channel) {
}

class UcxServerSocketChannelConfig(channel: AbstractUcxChannel)
    extends UcxChannelConfig(channel)
    with ServerSocketChannelConfig {

    override def setConnectTimeoutMillis(connectTimeoutMillis: Int): this.type = {
        super.setConnectTimeoutMillis(connectTimeoutMillis)
        this
    }

    override def setMaxMessagesPerRead(masetMaxMessagesPerReadxMessagesPerRead: Int): this.type = {
        // super.setMaxMessagesPerRead(maxMessagesPerRead)
        this
    }

    override def setWriteSpinCount(writeSpinCount: Int): this.type = {
        super.setWriteSpinCount(writeSpinCount)
        this
    }

    override def setAllocator(allocator: ByteBufAllocator): this.type = {
        super.setAllocator(allocator)
        this
    }

    override def setRecvByteBufAllocator(recvByteBufAllocator: RecvByteBufAllocator): this.type = {
        super.setRecvByteBufAllocator(recvByteBufAllocator)
        this
    }

    override def setAutoRead(autoRead: Boolean): this.type = {
        super.setAutoRead(autoRead)
        this
    }

    override def setAutoClose(autoClose: Boolean): this.type = {
        super.setAutoClose(autoClose)
        this
    }

    override def setWriteBufferHighWaterMark(writeBufferHighWaterMark: Int): this.type = {
        super.setWriteBufferHighWaterMark(writeBufferHighWaterMark)
        this
    }

    override def setWriteBufferLowWaterMark(writeBufferLowWaterMark: Int): this.type = {
        super.setWriteBufferLowWaterMark(writeBufferLowWaterMark)
        this
    }

    override def setWriteBufferWaterMark(writeBufferWaterMark: WriteBufferWaterMark): this.type = {
        super.setWriteBufferWaterMark(writeBufferWaterMark)
        this
    }

    override def setMessageSizeEstimator(estimator: MessageSizeEstimator): this.type = {
        super.setMessageSizeEstimator(estimator)
        this
    }

    def getBacklog(): Int = ???
    def getReceiveBufferSize(): Int = ???
    def isReuseAddress(): Boolean = ???
    def setBacklog(x$1: Int): this.type = this
    def setPerformancePreferences(x$1: Int,x$2: Int,x$3: Int): this.type = this
    def setReceiveBufferSize(x$1: Int): this.type = this
    def setReuseAddress(x$1: Boolean): this.type = this
}

class UcxSocketChannelConfig(channel: AbstractUcxChannel)
    extends UcxChannelConfig(channel)
    with SocketChannelConfig {

    override def setConnectTimeoutMillis(connectTimeoutMillis: Int): this.type = {
        super.setConnectTimeoutMillis(connectTimeoutMillis)
        this
    }

    override def setMaxMessagesPerRead(maxMessagesPerRead: Int): this.type = {
        // super.setMaxMessagesPerRead(maxMessagesPerRead)
        this
    }

    override def setWriteSpinCount(writeSpinCount: Int): this.type = {
        super.setWriteSpinCount(writeSpinCount)
        this
    }

    override def setAllocator(allocator: ByteBufAllocator): this.type = {
        super.setAllocator(allocator)
        this
    }

    override def setRecvByteBufAllocator(recvByteBufAllocator: RecvByteBufAllocator): this.type = {
        super.setRecvByteBufAllocator(recvByteBufAllocator)
        this
    }

    override def setAutoRead(autoRead: Boolean): this.type = {
        super.setAutoRead(autoRead)
        this
    }

    override def setAutoClose(autoClose: Boolean): this.type = {
        super.setAutoClose(autoClose)
        this
    }

    override def setWriteBufferHighWaterMark(writeBufferHighWaterMark: Int): this.type = {
        super.setWriteBufferHighWaterMark(writeBufferHighWaterMark)
        this
    }

    override def setWriteBufferLowWaterMark(writeBufferLowWaterMark: Int): this.type = {
        super.setWriteBufferLowWaterMark(writeBufferLowWaterMark)
        this
    }

    override def setWriteBufferWaterMark(writeBufferWaterMark: WriteBufferWaterMark): this.type = {
        super.setWriteBufferWaterMark(writeBufferWaterMark)
        this
    }

    override def setMessageSizeEstimator(estimator: MessageSizeEstimator): this.type = {
        super.setMessageSizeEstimator(estimator)
        this
    }

    def getReceiveBufferSize(): Int = ???
    def getSendBufferSize(): Int = ???
    def getSoLinger(): Int = ???
    def getTrafficClass(): Int = ???
    def isAllowHalfClosure(): Boolean = ???
    def isKeepAlive(): Boolean = ???
    def isReuseAddress(): Boolean = ???
    def isTcpNoDelay(): Boolean = ???
    def setAllowHalfClosure(x$1: Boolean): this.type = this
    def setKeepAlive(x$1: Boolean): this.type = this
    def setPerformancePreferences(x$1: Int,x$2: Int,x$3: Int): this.type = this
    def setReceiveBufferSize(x$1: Int): this.type = this
    def setReuseAddress(x$1: Boolean): this.type = this
    def setSendBufferSize(x$1: Int): this.type = this
    def setSoLinger(x$1: Int): this.type = this
    def setTcpNoDelay(x$1: Boolean): this.type = this
    def setTrafficClass(x$1: Int): this.type = this
}
