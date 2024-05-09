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

import java.util.concurrent.ExecutorService;

import io.netty.channel.Channel;

import org.apache.spark.network.protocol.ResponseMessage;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[NettyUcxTransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class NettyUcxTransportResponseHandler extends TransportResponseHandler {
  ExecutorService executor;

  public NettyUcxTransportResponseHandler(Channel channel, ExecutorService executor) {
    super(channel);
    this.executor = executor;
  }

  // @Override
  // public void handle(ResponseMessage message) throws Exception {
  //   executor.execute(new Runnable() {
  //     @Override
  //     public void run() {
  //       try {
  //         NettyUcxTransportResponseHandler.super.handle(message);
  //       } catch (Exception e) {
  //         throw new RuntimeException(e);
  //       }
  //     }
  //   });
  // }
}
