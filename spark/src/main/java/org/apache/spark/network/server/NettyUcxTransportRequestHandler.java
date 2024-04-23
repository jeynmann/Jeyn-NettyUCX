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

package org.apache.spark.network.server;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.*;
import org.apache.spark.network.protocol.*;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class NettyUcxTransportRequestHandler extends TransportRequestHandler {

  private static final Logger logger = LoggerFactory.getLogger(NettyUcxTransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  Channel channel;

  /** Client on the same channel allowing us to talk back to the requester. */
  TransportClient reverseClient;

  /** Handles all RPC messages. */
  RpcHandler rpcHandler;

  /** Returns each chunk part of a stream. */
  StreamManager streamManager;

  /** The max number of chunks being transferred and not finished yet. */
  long maxChunksBeingTransferred;

  public NettyUcxTransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred) {
    super(channel, reverseClient, rpcHandler, maxChunksBeingTransferred);
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequestInBatch((ChunkFetchRequest) request);
    } else {
      super.handle(request);
    }
  }

  void processFetchRequestInBatch(final ChunkFetchRequest req) {
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        req.streamChunkId);
    }
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      return;
    }

    long streamId = req.streamChunkId.streamId;
    int chunkNums = req.streamChunkId.chunkIndex;
    for (int chunkIndex = 0; chunkIndex != chunkNums; ++chunkIndex) {
        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        ManagedBuffer buf;
        try {
          streamManager.checkAuthorization(reverseClient, streamId);
          buf = streamManager.getChunk(streamId, chunkIndex);
        } catch (Exception e) {
          logger.error(String.format("Error opening block %s for request from %s",
            streamChunkId, getRemoteAddress(channel)), e);
            writeAndFlush(new ChunkFetchFailure(streamChunkId, Throwables.getStackTraceAsString(e)));
          return;
        }
        
        streamManager.chunkBeingSent(streamId);
        writeAndFlush(new ChunkFetchSuccess(streamChunkId, buf)).addListener(future -> {
          streamManager.chunkSent(streamId);
        });
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  ChannelFuture writeAndFlush(Encodable result) {
    return channel.writeAndFlush(result).addListener(future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, channel.remoteAddress());
      } else {
        logger.error(String.format("Error sending result %s to %s; closing connection",
          result, channel.remoteAddress()), future.cause());
        channel.close();
      }
    });
  }
}
