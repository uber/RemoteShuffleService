package com.uber.rss.handlers;

import com.uber.m3.tally.Stopwatch;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.tools.FileDescriptorStressTest;
import com.uber.rss.util.ExceptionUtils;
import com.uber.rss.util.FileUtils;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.util.concurrent.RateLimiter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

import static io.netty.buffer.Unpooled.copiedBuffer;

// Based upon https://github.com/zoomulus/servers (MIT license)

public class HttpChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(HttpChannelInboundHandler.class);

    private static final RateLimiter nonHealthCheckRateLimiter = RateLimiter.create(10);

    private static final long MIN_TOTAL_DISK_SPACE = 50L * 1024L * 1024L * 1024L; // 50GB
    private static final long MIN_FREE_DISK_SPACE = 10L * 1024L * 1024L * 1024L; // 10GB

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            final FullHttpRequest request = (FullHttpRequest) msg;

            HttpResponseStatus status;
            String responseMessage;
            // TODO More detailed HTTP server, for now it's just the health check
            if (request.uri().startsWith("/health")) {
                Stopwatch checkFreeSpaceLatencyTimerStopwatch = M3Stats.getDefaultScope().timer("checkFreeSpaceLatency").start();
                try {
                    logger.info("Hit /health endpoint sysenv: " + System.getenv("UBER_HEALTH_CHECK_TIMEOUT_RSS"));
                    // disable checkDiskFreeSpace for long time spending to minute level
                    // FileUtils.checkDiskFreeSpace(MIN_TOTAL_DISK_SPACE, MIN_FREE_DISK_SPACE);
                } catch(Throwable ex) {
                    logger.error("Failed at checkDiskFreeSpace", ex);
                    M3Stats.addException(ex, this.getClass().getSimpleName()+"CheckDiskFreeSpace");
                    throw ex;
                } finally {
                    responseMessage = "OK";
                    status = HttpResponseStatus.OK;
                    checkFreeSpaceLatencyTimerStopwatch.stop();
                }
            } else if (request.uri().startsWith("/threadDump")) {
                nonHealthCheckRateLimiter.acquire();
                responseMessage = Arrays.stream(org.apache.spark.util.Utils.getThreadDump())
                        .map(t->String.valueOf(t))
                        .collect(Collectors.joining(System.lineSeparator() + "----------" + System.lineSeparator()));
                status = HttpResponseStatus.OK;
            } else {
                nonHealthCheckRateLimiter.acquire();
                responseMessage = String.format("%s not found", request.uri());
                status = HttpResponseStatus.NOT_FOUND;
            }
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                    copiedBuffer(responseMessage.getBytes()));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, responseMessage.length());

            if (HttpUtil.isKeepAlive(request)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                ctx.writeAndFlush(response, ctx.voidPromise());
            } else {
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        M3Stats.addException(cause, this.getClass().getSimpleName());
        logger.warn("HTTPHandler got exception", cause);
        ctx.writeAndFlush(new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, copiedBuffer(ExceptionUtils.getSimpleMessage(cause).getBytes(StandardCharsets.UTF_8))
        )).addListener(ChannelFutureListener.CLOSE);
    }
}
