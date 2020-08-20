package com.uber.rss.handlers;

import com.uber.rss.common.LogWrapper;
import com.uber.rss.messages.FinishApplicationJobRequestMessage;
import com.uber.rss.messages.FinishApplicationAttemptRequestMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ConnectNotifyRequest;
import com.uber.rss.messages.ConnectNotifyResponse;
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

    private String user;

    public NotifyServerHandler(String serverId) {
        this.serverId = serverId;
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
        LogWrapper.logAsJson(logger,
                "event",
                "finishApplicationAttempt",
                "appId",
                msg.getAppId(),
                "appAttempt",
                msg.getAppAttempt());

        writeAndFlushByte(ctx, MessageConstants.RESPONSE_STATUS_OK);

        ApplicationMetrics metrics = metricsContainer.getApplicationMetrics(user, msg.getAppAttempt());
        metrics.getNumApplications().inc(1);
    }

    private void writeAndFlushByte(ChannelHandlerContext ctx, byte value) {
        ByteBuf buf = ctx.alloc().buffer(1);
        buf.writeByte(value);
        ctx.writeAndFlush(buf);
    }
}
