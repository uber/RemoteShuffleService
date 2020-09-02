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
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.FilePathAndLength;
import com.uber.rss.common.MapTaskCommitStatus;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.messages.BaseMessage;
import com.uber.rss.messages.ConnectDownloadRequest;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ConnectDownloadResponse;
import com.uber.rss.messages.GetDataAvailabilityRequest;
import com.uber.rss.messages.GetDataAvailabilityResponse;
import com.uber.rss.messages.ShuffleStageStatus;
import com.uber.rss.messages.StartUploadMessage;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.NettyUtils;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DownloadChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(DownloadChannelInboundHandler.class);

    private static Counter numChannelActive = M3Stats.getDefaultScope().counter("numDownloadChannelActive");
    private static Counter numChannelInactive = M3Stats.getDefaultScope().counter("numDownloadChannelInactive");

    private static AtomicInteger concurrentChannelsAtomicInteger = new AtomicInteger();
    private static Gauge numConcurrentChannels = M3Stats.getDefaultScope().gauge("numConcurrentDownloadChannels");

    private final String serverId;
    private final String runningVersion;

    private final DownloadServerHandler downloadServerHandler;

    private String connectionInfo = "";
    private AppShufflePartitionId appShufflePartitionId = null;
    private List<Long> knownLatestTaskAttemptIds = new ArrayList<>();

    private StartUploadMessage startUploadMessage = null;

    public DownloadChannelInboundHandler(String serverId,
                                         String runningVersion,
                                         ShuffleExecutor executor) {
        this.serverId = serverId;
        this.runningVersion = runningVersion;
        this.downloadServerHandler = new DownloadServerHandler(executor);
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        processChannelActive(ctx);
    }

    public void processChannelActive(final ChannelHandlerContext ctx) {
        numChannelActive.inc(1);
        numConcurrentChannels.update(concurrentChannelsAtomicInteger.incrementAndGet());
        connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
        logger.info("Channel active: {}", connectionInfo);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        
        numChannelInactive.inc(1);
        numConcurrentChannels.update(concurrentChannelsAtomicInteger.decrementAndGet());
        logger.info("Channel inactive: {}", connectionInfo);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            logger.debug("Got incoming message: {}, {}", msg, connectionInfo);

            // Process other messages. We assume the header messages are already processed, thus some fields of this
            // class are already populated with proper values, e.g. user field.

            if (msg instanceof ConnectDownloadRequest) {
                logger.info("ConnectDownloadRequest: {}, {}", msg, connectionInfo);

                ConnectDownloadRequest connectRequest = (ConnectDownloadRequest) msg;
                appShufflePartitionId = new AppShufflePartitionId(
                    connectRequest.getAppId(),
                    connectRequest.getAppAttempt(),
                    connectRequest.getShuffleId(),
                    connectRequest.getPartitionId()
                );
                knownLatestTaskAttemptIds = connectRequest.getTaskAttemptIds();

                ShuffleStageStatus shuffleStageStatus = downloadServerHandler.getShuffleStageStatus(appShufflePartitionId.getAppShuffleId());
                if (shuffleStageStatus.getFileStatus() == ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED) {
                    logger.warn(String.format("Shuffle stage not started for %s, %s", appShufflePartitionId.getAppShuffleId(), connectionInfo));
                    HandlerUtil.writeResponseStatus(ctx, MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED);
                    return;
                }

                ShuffleWriteConfig config;
                try {
                    config = downloadServerHandler.getShuffleWriteConfig(appShufflePartitionId.getAppShuffleId());
                } catch (RssShuffleStageNotStartedException ex) {
                    logger.warn(String.format("Shuffle stage not started for %s, %s", appShufflePartitionId.getAppShuffleId(), connectionInfo));
                    HandlerUtil.writeResponseStatus(ctx, MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED);
                    return;
                }

                downloadServerHandler.initialize(connectRequest);

                String fileCompressionCodec = config.getFileCompressionCodec();
                MapTaskCommitStatus mapTaskCommitStatus = shuffleStageStatus.getMapTaskCommitStatus();
                boolean dataAvailable = mapTaskCommitStatus != null && mapTaskCommitStatus.isPartitionDataAvailable(knownLatestTaskAttemptIds);
                ConnectDownloadResponse connectResponse = new ConnectDownloadResponse(serverId, RssBuildInfo.Version, runningVersion, fileCompressionCodec, mapTaskCommitStatus, dataAvailable);
                sendResponseAndFiles(ctx, dataAvailable, shuffleStageStatus, connectResponse);
            } else if (msg instanceof GetDataAvailabilityRequest) {
                ShuffleStageStatus shuffleStageStatus = downloadServerHandler.getShuffleStageStatus(appShufflePartitionId.getAppShuffleId());
                MapTaskCommitStatus mapTaskCommitStatus = shuffleStageStatus.getMapTaskCommitStatus();
                boolean dataAvailable;
                dataAvailable = mapTaskCommitStatus != null && mapTaskCommitStatus.isPartitionDataAvailable(knownLatestTaskAttemptIds);
                GetDataAvailabilityResponse getDataAvailabilityResponse = new GetDataAvailabilityResponse(mapTaskCommitStatus, dataAvailable);
                sendResponseAndFiles(ctx, dataAvailable, shuffleStageStatus, getDataAvailabilityResponse);
            } else {
                throw new RssInvalidDataException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        M3Stats.addException(cause, "serverHandler");
        String msg = "Got exception " + connectionInfo;
        logger.warn(msg, cause);
        ctx.close();
    }

    // send response to client, also send files if data is available
    private void sendResponseAndFiles(ChannelHandlerContext ctx, boolean dataAvailable, ShuffleStageStatus shuffleStageStatus, BaseMessage responseMessage) {
        byte responseStatus = shuffleStageStatus.transformToMessageResponseStatus();
        if (dataAvailable) {
            List<FilePathAndLength> files = downloadServerHandler.getNonEmptyPartitionFiles(connectionInfo);
            ChannelFuture channelFuture = HandlerUtil.writeResponseMsg(ctx, responseStatus, responseMessage, true);
            if (shuffleStageStatus.getFileStatus() == ShuffleStageStatus.FILE_STATUS_CORRUPTED || files.isEmpty()) {
                channelFuture.addListener(ChannelFutureListener.CLOSE);
            } else {
                downloadServerHandler.sendFiles(ctx, files);
            }
        } else {
            ChannelFuture channelFuture = HandlerUtil.writeResponseMsg(ctx, responseStatus, responseMessage, true);
            if (shuffleStageStatus.getFileStatus() == ShuffleStageStatus.FILE_STATUS_CORRUPTED) {
                channelFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
