/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network;

import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.NettyUcxTransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.NettyUcxTransportClientFactory;
import org.apache.spark.network.client.NettyUcxTransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.NettyUcxMessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.NettyUcxTransportRequestHandler;
import org.apache.spark.network.server.NettyUcxTransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link NettyUcxTransportServer}, {@link NettyUcxTransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the NettyUcxTransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * NettyUcxTransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The NettyUcxTransportServer and NettyUcxTransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a NettyUcxTransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class NettyUcxTransportContext extends TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(NettyUcxTransportContext.class);

  /**
   * Force to create NettyUcxMessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  private static final NettyUcxMessageEncoder ENCODER = NettyUcxMessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  public static final String FILE_FRAME_SIZE_KEY = "spark.shuffle.io.fileFrameSize";
  public static final int FILE_FRAME_SIZE_DEFAULT = 32 << 10;

  TransportConf conf;
  RpcHandler rpcHandler;
  boolean closeIdleConnections;

  public NettyUcxTransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  public NettyUcxTransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    super(conf, rpcHandler, closeIdleConnections);
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;
  }

  public int fileFrameSize() {
    return conf.getInt(FILE_FRAME_SIZE_KEY, FILE_FRAME_SIZE_DEFAULT);
  }

//   /**
//    * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
//    * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
//    * to create a Client.
//    */
//   @Override
//   public NettyUcxTransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
//     return new NettyUcxTransportClientFactory(this, bootstraps);
//   }

//   @Override
//   /** Create a server which will attempt to bind to a specific port. */
//   public NettyUcxTransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
//     return new NettyUcxTransportServer(this, null, port, rpcHandler, bootstraps);
//   }

//   @Override
//   /** Create a server which will attempt to bind to a specific host and port. */
//   public NettyUcxTransportServer createServer(
//       String host, int port, List<TransportServerBootstrap> bootstraps) {
//     return new NettyUcxTransportServer(this, host, port, rpcHandler, bootstraps);
//   }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a NettyUcxTransportClient that can
   * be used to communicate on this channel. The NettyUcxTransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same NettyUcxTransportClient object.
   */
  @Override
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    NettyUcxTransportResponseHandler responseHandler = new NettyUcxTransportResponseHandler(channel);
    NettyUcxTransportClient client = new NettyUcxTransportClient(channel, responseHandler);
    NettyUcxTransportRequestHandler requestHandler = new NettyUcxTransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred());
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }
}
