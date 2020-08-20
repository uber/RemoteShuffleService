/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.handlers;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.rss.RssBuildInfo;
import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssMaxConnectionsException;
import com.uber.rss.exceptions.RssTooMuchDataException;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.messages.FinishUpload2Message;
import com.uber.rss.messages.FinishUploadMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ShuffleDataWrapper;
import com.uber.rss.messages.CloseConnectionMessage;
import com.uber.rss.messages.ConnectUploadRequest;
import com.uber.rss.messages.ConnectUploadResponse;
import com.uber.rss.messages.StartUploadMessage;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.ExceptionUtils;
import com.uber.rss.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class UploadChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(UploadChannelInboundHandler.class);

    private static Counter numChannelActive = M3Stats.getDefaultScope().counter("numUploadChannelActive");
    private static Counter numChannelInactive = M3Stats.getDefaultScope().counter("numUploadChannelInactive");

    private static AtomicInteger concurrentChannelsAtomicInteger = new AtomicInteger();
    private static Gauge numConcurrentChannels = M3Stats.getDefaultScope().gauge("numConcurrentUploadChannels");
    private static Gauge finishUploadRequestLag = M3Stats.getDefaultScope().gauge("finishUploadRequestLag");

    private static Counter closedIdleUploadChannels = M3Stats.getDefaultScope().counter("closedIdleUploadChannels");

    private final String serverId;
    private final String runningVersion;

    private final long idleTimeoutMillis;

    private final UploadServerHandler uploadServerHandler;

    private String connectionInfo = "";
    private String user = "unknown";
    private String appId = null;
    private String appAttempt = null;

    private StartUploadMessage startUploadMessage = null;

    private IdleCheck idleCheck;

    public UploadChannelInboundHandler(String serverId,
                                       String runningVersion,
                                       long idleTimeoutMillis,
                                       ShuffleExecutor executor,
                                       UploadChannelManager channelManager,
                                       String networkCompressionCodec) {
        this.serverId = serverId;
        this.runningVersion = runningVersion;
        this.uploadServerHandler = new UploadServerHandler(serverId, runningVersion, executor, channelManager, networkCompressionCodec);
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        processChannelActive(ctx);
    }

    public void processChannelActive(final ChannelHandlerContext ctx) {
        logger.info(String.format("Channel active: %s", connectionInfo));
        numChannelActive.inc(1);
        numConcurrentChannels.update(concurrentChannelsAtomicInteger.incrementAndGet());
        connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

        idleCheck = new IdleCheck(ctx, idleTimeoutMillis);
        schedule(ctx, idleCheck, idleTimeoutMillis);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        logger.info(String.format("Channel inactive: %s", connectionInfo));
        numChannelInactive.inc(1);
        numConcurrentChannels.update(concurrentChannelsAtomicInteger.decrementAndGet());
        uploadServerHandler.onChannelInactive();

        if (idleCheck != null) {
            idleCheck.cancel();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Got incoming message: " + msg + ", " + connectionInfo);
            }

            if (idleCheck != null) {
                idleCheck.updateLastReadTime();
            }

            // Process other messages. We assume the header messages are already processed, thus some fields of this
            // class are already populated with proper values, e.g. user field.

            if (msg instanceof ConnectUploadRequest) {
                logger.info(msg + ", " + connectionInfo);

                try {
                    uploadServerHandler.checkMaxConnections();
                } catch (RssMaxConnectionsException e) {
                    logger.info(String.format(
                        "Cannot handle new connection due to server capacity. Closing current connection: %s. %s",
                        connectionInfo, ExceptionUtils.getSimpleMessage(e)));
                    M3Stats.addException(e, M3Stats.TAG_VALUE_SERVER_HANDLER);
                    ByteBuf buf = ctx.alloc().buffer(1);
                    buf.writeByte(MessageConstants.RESPONSE_STATUS_SERVER_BUSY);
                    ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
                    return;
                }

                ConnectUploadRequest connectUploadRequest = (ConnectUploadRequest)msg;
                user = connectUploadRequest.getUser();
                appId = connectUploadRequest.getAppId();
                appAttempt = connectUploadRequest.getAppAttempt();

                try {
                    uploadServerHandler.checkAppMaxWriteBytes(appId);
                } catch (RssTooMuchDataException e) {
                    logger.info(String.format(
                        "Cannot handle new connection due to writing too much data from app (%s). Closing current connection: %s. %s",
                        appId, connectionInfo, ExceptionUtils.getSimpleMessage(e)),
                        e);
                    M3Stats.addException(e, M3Stats.TAG_VALUE_SERVER_HANDLER);
                    ByteBuf buf = ctx.alloc().buffer(1);
                    buf.writeByte(MessageConstants.RESPONSE_STATUS_APP_TOO_MUCH_DATA);
                    ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
                }

                ConnectUploadResponse connectUploadResponse = new ConnectUploadResponse(serverId, RssBuildInfo.Version, runningVersion);
                HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, connectUploadResponse, true);
            } else if (msg instanceof StartUploadMessage) {
                logger.debug(msg + ", " + connectionInfo);
                startUploadMessage = (StartUploadMessage)msg;

                AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId,
                    appAttempt,
                    startUploadMessage.getShuffleId(),
                    startUploadMessage.getMapId(),
                    startUploadMessage.getAttemptId());

                ShuffleWriteConfig writeConfig = new ShuffleWriteConfig(startUploadMessage.getFileCompressionCodec(), startUploadMessage.getNumSplits());
                uploadServerHandler.initializeAppTaskAttempt(appTaskAttemptId, startUploadMessage.getNumMaps(), startUploadMessage.getNumPartitions(), writeConfig, ctx);
            } else if (msg instanceof FinishUploadMessage) {
                logger.info(msg + ", " + connectionInfo);
                FinishUploadMessage finishUploadMessage = (FinishUploadMessage)msg;
                finishUploadRequestLag.update(System.currentTimeMillis() - finishUploadMessage.getTimestamp());
                uploadServerHandler.finishUpload(finishUploadMessage.getTaskAttemptId());
            } else if (msg instanceof FinishUpload2Message) {
                logger.info(msg + ", " + connectionInfo);
                FinishUpload2Message finishUploadMessage = (FinishUpload2Message)msg;
                finishUploadRequestLag.update(System.currentTimeMillis() - finishUploadMessage.getTimestamp());
                byte ackFlag = finishUploadMessage.getAckFlag();
                uploadServerHandler.finishUpload(finishUploadMessage.getTaskAttemptId());
                if (ackFlag != FinishUpload2Message.ACK_FLAG_NO_ACK) {
                    ByteBuf buf = ctx.alloc().buffer(1);
                    buf.writeByte(MessageConstants.RESPONSE_STATUS_OK);
                    ctx.writeAndFlush(buf);
                }
            } else if (msg instanceof ShuffleDataWrapper) {
                ShuffleDataWrapper shuffleDataWrapper = (ShuffleDataWrapper)msg;
                uploadServerHandler.writeRecord(shuffleDataWrapper);
            } else if (msg instanceof CloseConnectionMessage) {
                logger.debug(msg + ", " + connectionInfo);
                ctx.close();
            } else {
                throw new RssInvalidDataException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
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

    private static void schedule(ChannelHandlerContext ctx, Runnable task, long delayMillis) {
        ctx.executor().schedule(task, delayMillis, TimeUnit.MILLISECONDS);
    }

    private static class IdleCheck implements Runnable {

        private final ChannelHandlerContext ctx;
        private final long idleTimeoutMillis;

        private volatile long lastReadTime = System.currentTimeMillis();
        private volatile boolean canceled = false;

        public IdleCheck(ChannelHandlerContext ctx, long idleTimeoutMillis) {
            this.ctx = ctx;
            this.idleTimeoutMillis = idleTimeoutMillis;
        }

        @Override
        public void run() {
            try {
                if (canceled) {
                    return;
                }

                if (!ctx.channel().isOpen()) {
                    return;
                }

                checkIdle(ctx);
            } catch (Throwable ex) {
                logger.warn(String.format("Failed to run idle check, %s", NettyUtils.getServerConnectionInfo(ctx)), ex);
            }
        }

        public void updateLastReadTime() {
            lastReadTime = System.currentTimeMillis();
        }

        public void cancel() {
            canceled = true;
        }

        private void checkIdle(ChannelHandlerContext ctx) {
            if (System.currentTimeMillis() - lastReadTime >= idleTimeoutMillis) {
                closedIdleUploadChannels.inc(1);
                logger.info(String.format("Closing idle connection %s", NettyUtils.getServerConnectionInfo(ctx)));
                ctx.close();
                return;
            }

            schedule(ctx, this, idleTimeoutMillis);
        }
    }
}
