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

package org.apache.spark.network.client;

import java.io.IOException;
import java.util.UUID;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.*;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of NettyUcxTransportClient using {@link NettyUcxTransportClientFactory}. A single
 * NettyUcxTransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class NettyUcxTransportClient extends TransportClient {
  private static final Logger logger = LoggerFactory.getLogger(NettyUcxTransportClient.class);

  Channel channel;
  TransportResponseHandler handler;

  public NettyUcxTransportClient(Channel channel, TransportResponseHandler handler) {
    super(channel, handler);
    this.channel = channel;
    this.handler = handler;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunks requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * NettyUcxTransportClient is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkNums number of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchBatch(
      long streamId,
      int chunkNums,
      ChunkReceivedCallback callback) {
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkNums, getRemoteAddress(channel));
    }

    StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkNums);
    StdChannelListener listener = new StdChannelListener(streamChunkId) {
      @Override
      void handleFailure(String errorMsg, Throwable cause) {
        for (int chunkIndex = 0; chunkIndex != chunkNums; ++chunkIndex) {
            handler.removeFetchRequest(new StreamChunkId(streamId, chunkIndex));
            callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
        }
      }
    };
    for (int i = 0; i != chunkNums; ++i) {
      handler.addFetchRequest(new StreamChunkId(streamId, i), callback);
    }
    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
  }

  static long requestId() {
    return Math.abs(UUID.randomUUID().getLeastSignificantBits());
  }

  class StdChannelListener
      implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isTraceEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.trace("Sending request {} to {} took {} ms", requestId,
              getRemoteAddress(channel), timeTaken);
        }
      } else {
        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
            getRemoteAddress(channel), future.cause());
        logger.error(errorMsg, future.cause());
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    void handleFailure(String errorMsg, Throwable cause) throws Exception {}
  }
}
