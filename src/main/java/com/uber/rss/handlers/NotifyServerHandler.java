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

import com.uber.rss.common.AppShuffleId;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.messages.FinishApplicationJobRequestMessage;
import com.uber.rss.messages.FinishApplicationAttemptRequestMessage;
import com.uber.rss.messages.FinishApplicationStageRequestMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ConnectNotifyRequest;
import com.uber.rss.messages.ConnectNotifyResponse;
import com.uber.rss.messages.ShuffleStageStatus;
import com.uber.rss.metrics.ApplicationJobStatusMetrics;
import com.uber.rss.metrics.ApplicationMetrics;
import com.uber.rss.metrics.NotifyServerMetricsContainer;
import com.uber.rss.util.MonitorUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * This class handles messages as a notify server, which accepts some notification from shuffle client.
 */
public class NotifyServerHandler {
    private static final Logger logger = LoggerFactory.getLogger(NotifyServerHandler.class);

    private static final NotifyServerMetricsContainer metricsContainer = new NotifyServerMetricsContainer();

    private final String serverId;
    private final ShuffleExecutor executor;

    private String user;

    public NotifyServerHandler(String serverId, ShuffleExecutor executor) {
        this.serverId = serverId;
        this.executor = executor;
    }

    public void handleMessage(ChannelHandlerContext ctx, ConnectNotifyRequest msg) {
        logger.debug("Handle message: " + msg);
        user = msg.getUser();

        ConnectNotifyResponse response = new ConnectNotifyResponse(serverId);
        HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
    }

    public void handleMessage(ChannelHandlerContext ctx, FinishApplicationJobRequestMessage msg) {
        writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

        ApplicationJobStatusMetrics metrics = metricsContainer.getApplicationJobStatusMetrics(user, msg.getJobStatus());
        metrics.getNumApplicationJobs().inc(1);

        String exceptionDetail = msg.getExceptionDetail();
        if (MonitorUtils.hasRssException(exceptionDetail)) {
            metrics.getNumRssExceptionJobs().inc(1);
        }
    }

    public void handleMessage(ChannelHandlerContext ctx, FinishApplicationAttemptRequestMessage msg) {
        logger.info("finishApplicationAttempt, appId: {}, appAttempt: {}", msg.getAppId(), msg.getAppAttempt());

        writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

        ApplicationMetrics metrics = metricsContainer.getApplicationMetrics(user, msg.getAppAttempt());
        metrics.getNumApplications().inc(1);
    }

    public void handleMessage(ChannelHandlerContext ctx, FinishApplicationStageRequestMessage msg) {
        writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

        logger.info("finishApplicationStage, appId: {}, appAttempt: {}, stageId: {}",
            msg.getAppId(),
            msg.getAppAttempt(),
            msg.getStageId());

        // TODO investigate further whether stageId->shuffleId is 1-1. initial investigations suggest so but would
        // be worth knowing 100%
        AppShuffleId shuffleId;
        try  {
            shuffleId = executor.getShuffleId(msg.getStageId());
        } catch (RssException e) {
            logger.debug("Shuffle Stage {} does not do any writing", msg.getStageId(), e);
            return;
        }

        ShuffleStageStatus status = executor.getShuffleStageStatus(shuffleId);
        if (status.getFileStatus() == ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED) {
            // This case "should" never occur unless thread handling uploadMessage got stuck and this ran first
            String error = String.format("Shuffle stage was not started for stage=%s shuffle=%s unable to close shuffle files", msg.getStageId(), shuffleId);
            logger.error(error);
            throw new RssShuffleStageNotStartedException(error);
        }

        // TODO investigate whether its possible for next stage to start before this handler is done running
        // in current rss implementation, this would be a problem as download requests start before the shuffle
        // files had made it to a storage like s3 which is slower than local or hdfs
        executor.getStageState(shuffleId).closeWriters();

        logger.info("writing is complete for stage= {}, shuffleId= {} ", msg.getStageId(), shuffleId);
    }

    private void writeAndFlushByte(ChannelHandlerContext ctx, byte value) {
        ByteBuf buf = ctx.alloc().buffer(1);
        buf.writeByte(value);
        ctx.writeAndFlush(buf);
    }
}
