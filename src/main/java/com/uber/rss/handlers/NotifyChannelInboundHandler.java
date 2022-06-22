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

package com.uber.rss.handlers;

import com.uber.rss.messages.ConnectNotifyRequest;
import com.uber.rss.messages.FinishApplicationAttemptRequestMessage;
import com.uber.rss.messages.FinishApplicationJobRequestMessage;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.NettyUtils;
import com.uber.rss.exceptions.RssInvalidDataException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotifyChannelInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(NotifyChannelInboundHandler.class);

  private String connectionInfo = "";

  private final NotifyServerHandler serverHandler;

  public NotifyChannelInboundHandler(String serverId) {
    serverHandler = new NotifyServerHandler(serverId);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    processChannelActive(ctx);
  }

  public void processChannelActive(final ChannelHandlerContext ctx) {
    connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    try {
      if (logger.isTraceEnabled()) {
        logger.trace("Got incoming message: " + msg + ", " + connectionInfo);
      }

      if (msg instanceof ConnectNotifyRequest) {
        serverHandler.handleMessage(ctx, (ConnectNotifyRequest) msg);
      } else if (msg instanceof FinishApplicationJobRequestMessage) {
        serverHandler.handleMessage(ctx, (FinishApplicationJobRequestMessage) msg);
      } else if (msg instanceof FinishApplicationAttemptRequestMessage) {
        serverHandler.handleMessage(ctx, (FinishApplicationAttemptRequestMessage) msg);
      } else {
        throw new RssInvalidDataException(
            String.format("Unsupported message: %s, %s", msg, connectionInfo));
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    M3Stats.addException(cause, this.getClass().getSimpleName());
    String msg = "Got exception " + connectionInfo;
    logger.warn(msg, cause);
    ctx.close();
  }

}
