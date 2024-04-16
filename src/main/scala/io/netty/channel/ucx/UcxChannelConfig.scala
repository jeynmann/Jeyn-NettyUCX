package io.netty.channel.ucx

import io.netty.channel.DefaultChannelConfig
import io.netty.channel.WriteBufferWaterMark
import io.netty.channel.MessageSizeEstimator
import io.netty.channel.RecvByteBufAllocator
import io.netty.channel.socket.ServerSocketChannelConfig
import io.netty.channel.socket.SocketChannelConfig

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.UcxPooledByteBufAllocator

class UcxChannelConfig(channel: AbstractUcxChannel)
    extends DefaultChannelConfig(channel) {
}

class UcxServerSocketChannelConfig(channel: AbstractUcxChannel)
    extends UcxChannelConfig(channel)
    with ServerSocketChannelConfig {
    setAllocator(UcxPooledByteBufAllocator.DEFAULT)

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
        assert(allocator.isInstanceOf[UcxPooledByteBufAllocator])
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

    def getBacklog(): Int = backlog

    def getReceiveBufferSize(): Int = receiveBufferSize

    def isReuseAddress(): Boolean = reuseAddress

    def setBacklog(x: Int): this.type = {
        backlog = x
        this
    }

    def setPerformancePreferences(x1: Int, x2: Int, x3: Int): this.type = {
        performancePreferences = Array(x1, x2, x3)
        this
    }

    def setReceiveBufferSize(x: Int): this.type = {
        receiveBufferSize = x
        this
    }

    def setReuseAddress(x: Boolean): this.type = {
        reuseAddress = x
        this
    }

    private var backlog = 0
    private var receiveBufferSize = 0
    private var reuseAddress = true
    private var performancePreferences = Array(0, 0, 0)
}

class UcxSocketChannelConfig(channel: AbstractUcxChannel)
    extends UcxChannelConfig(channel)
    with SocketChannelConfig {
    setAllocator(UcxPooledByteBufAllocator.DEFAULT)

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
        assert(allocator.isInstanceOf[UcxPooledByteBufAllocator])
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

    def getReceiveBufferSize(): Int = receiveBufferSize

    def getSendBufferSize(): Int = sendBufferSize

    def getSoLinger(): Int = soLinger

    def getTrafficClass(): Int = trafficClass

    def getFileFrameSize(): Int = fileFrameSize

    def isAllowHalfClosure(): Boolean = allowHalfClosure

    def isKeepAlive(): Boolean = keepAlive

    def isReuseAddress(): Boolean = reuseAddress

    def isTcpNoDelay(): Boolean = tcpNoDelay

    def setAllowHalfClosure(x: Boolean): this.type = {
        allowHalfClosure = x
        this
    }

    def setKeepAlive(x: Boolean): this.type = {
        allowHalfClosure = x
        this
    }

    def setPerformancePreferences(x1: Int, x2: Int, x3: Int): this.type = {
        performancePreferences = Array(x1, x2, x3)
        this
    }

    def setReceiveBufferSize(x: Int): this.type = {
        receiveBufferSize = x
        this
    }

    def setReuseAddress(x: Boolean): this.type = {
        reuseAddress = x
        this
    }

    def setSendBufferSize(x: Int): this.type = {
        sendBufferSize = x
        this
    }

    def setSoLinger(x: Int): this.type = {
        soLinger = x
        this
    }

    def setTcpNoDelay(x: Boolean): this.type = {
        tcpNoDelay = x
        this
    }

    def setTrafficClass(x: Int): this.type = {
        trafficClass = x
        this
    }

    def setFileFrameSize(frameSize: Int): this.type = {
        fileFrameSize = frameSize
        this
    }

    private var fileFrameSize = 32 << 10
    // private var fileFrameSize = 4 << 20
    private var receiveBufferSize = 0
    private var sendBufferSize = 0
    private var soLinger = 0
    private var trafficClass = 0
    private var allowHalfClosure = true
    private var keepAlive = false
    private var reuseAddress = true
    private var tcpNoDelay = false
    private var performancePreferences = Array(0, 0, 0)
}
