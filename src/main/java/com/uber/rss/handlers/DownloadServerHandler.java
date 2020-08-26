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
import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.FilePathAndLength;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssShuffleCorruptedException;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.messages.ConnectDownloadRequest;
import com.uber.rss.messages.ShuffleStageStatus;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.storage.ShuffleFileStorage;
import com.uber.rss.storage.ShuffleStorage;
import com.uber.rss.util.LogUtils;
import com.uber.rss.util.NettyUtils;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.DefaultFileRegion;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/***
 * This class processes client request to download shuffle data.
 */
public class DownloadServerHandler {
    private static final Logger logger = LoggerFactory.getLogger(DownloadServerHandler.class);

    private static final AtomicInteger numConcurrentReadFilesAtomicInteger = new AtomicInteger();
    private static final Gauge numConcurrentReadFiles = M3Stats.getDefaultScope().gauge("numConcurrentReadFiles");
    private static final Counter numReadFileBytes = M3Stats.getDefaultScope().counter("numReadFileBytes");

    private final ShuffleExecutor executor;
    private final ShuffleStorage storage = new ShuffleFileStorage();

    private AppShuffleId appShuffleId;
    private int partitionId;

    public DownloadServerHandler(ShuffleExecutor executor) {
        this.executor = executor;
    }

    public void initialize(ConnectDownloadRequest connectDownloadRequest) {
        this.appShuffleId = new AppShuffleId(
            connectDownloadRequest.getAppId(), connectDownloadRequest.getAppAttempt(), connectDownloadRequest.getShuffleId());
        this.partitionId = connectDownloadRequest.getPartitionId();
    }

    // may return null
    public ShuffleWriteConfig getShuffleWriteConfig(AppShuffleId appShuffleId) {
        return executor.getShuffleWriteConfig(appShuffleId);
    }

    public ShuffleStageStatus getShuffleStageStatus(AppShuffleId appShuffleId) {
        return executor.getShuffleStageStatus(appShuffleId);
    }

    public List<FilePathAndLength> getNonEmptyPartitionFiles(String connectionInfoForLogging) {
        if (!storage.isLocalStorage()) {
            throw new RssInvalidStateException("Only local file storage is supported to download shuffle data, closing the connection");
        }

        List<FilePathAndLength> persistedBytes = executor.getPersistedBytes(
            appShuffleId, partitionId)
            .stream()
            .filter(t->t.getLength() > 0)
            .collect(Collectors.toList());

        if (persistedBytes.isEmpty()) {
            return Collections.emptyList();
        }

        for (FilePathAndLength filePathAndLength: persistedBytes) {
            if (!storage.exists(filePathAndLength.getPath())) {
                throw new RssShuffleCorruptedException(String.format(
                    "Shuffle file %s not found for partition %s, %s, %s, but there are persisted bytes: %s",
                    filePathAndLength.getPath(), partitionId, appShuffleId, connectionInfoForLogging, filePathAndLength.getLength()));
            }
            long fileSize = storage.size(filePathAndLength.getPath());
            if (fileSize <= 0) {
                throw new RssShuffleCorruptedException(String.format(
                    "Shuffle file %s is empty for partition %s, %s, %s, but there are persisted bytes: %s",
                    filePathAndLength.getPath(), partitionId, appShuffleId, connectionInfoForLogging, filePathAndLength.getLength()));
            }
            if (fileSize < filePathAndLength.getLength()) {
                throw new RssShuffleCorruptedException(String.format(
                    "Shuffle file %s has less size %s than expected %s for partition %s, %s, %s",
                    filePathAndLength.getPath(), fileSize, filePathAndLength.getLength(), partitionId, appShuffleId, connectionInfoForLogging));
            }
        }

        long totalFileLength = persistedBytes.stream().mapToLong(t->t.getLength()).sum();
        if (totalFileLength == 0) {
            logger.info(
                "Total file length is zero: {}, {}",
                StringUtils.join(persistedBytes, ','), connectionInfoForLogging);
            return Collections.emptyList();
        } else if (totalFileLength < 0) {
            throw new RssInvalidStateException(String.format(
                "Invalid total file length: %s, %s",
                totalFileLength, connectionInfoForLogging));
        }

        // TODO verify there is no open files
        return persistedBytes;
    }

    public void sendFiles(ChannelHandlerContext ctx, List<FilePathAndLength> nonEmptyFiles) {
        String connectionInfo = NettyUtils.getServerConnectionInfo(ctx);

        for (int i = 0; i < nonEmptyFiles.size(); i++) {
            final int fileIndex = i;
            String splitFile = nonEmptyFiles.get(fileIndex).getPath();
            long fileLength = nonEmptyFiles.get(fileIndex).getLength();
            logger.info(
                "Downloader server sending file: {} ({} of {}, {} bytes), {}",
                splitFile, fileIndex + 1, nonEmptyFiles.size(), fileLength, connectionInfo);
            // TODO support HDFS in future? need to remove code depending
            // on local file: new File(path)
            // TODO is storage.size(splitFile) reliable or consistent when finishing writing a file?
            DefaultFileRegion fileRegion = new DefaultFileRegion(
                new File(splitFile), 0, fileLength);
            ChannelFuture sendFileFuture = ctx.writeAndFlush(fileRegion,
                ctx.newProgressivePromise());
            int numConcurrentReadFilesValue = numConcurrentReadFilesAtomicInteger.incrementAndGet();
            numConcurrentReadFiles.update(numConcurrentReadFilesValue);
            final long sendFileStartTime = System.currentTimeMillis();
            sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
                @Override
                public void operationComplete(ChannelProgressiveFuture future) throws Exception {
                    executor.updateLiveness(appShuffleId.getAppId());
                    int numConcurrentReadFilesValue = numConcurrentReadFilesAtomicInteger.decrementAndGet();
                    numConcurrentReadFiles.update(numConcurrentReadFilesValue);
                    numReadFileBytes.inc(fileLength);
                    String exceptionInfo = "";
                    Throwable futureException = future.cause();
                    if (futureException != null) {
                        M3Stats.addException(futureException, M3Stats.TAG_VALUE_DOWNLOAD_PROCESSOR);
                        exceptionInfo = String.format(
                            ", exception: %s, %s",
                            com.uber.rss.util.ExceptionUtils.getSimpleMessage(future.cause()),
                            ExceptionUtils.getStackTrace(future.cause()));
                    }
                    double dataSpeed = LogUtils.calculateMegaBytesPerSecond(System.currentTimeMillis() - sendFileStartTime, fileLength);
                    logger.info(
                        "Finished sending file: {} ({} of {}), success: {} ({} mbs, total {} bytes), connection: {} {}",
                        splitFile, fileIndex + 1, nonEmptyFiles.size(), future.isSuccess(), dataSpeed, fileLength, connectionInfo, exceptionInfo);

                    if (fileIndex == nonEmptyFiles.size() - 1) {
                        logger.debug(
                            "Closing server side channel after sending last file: {}, {}",
                            splitFile, connectionInfo);
                        future.channel().close();
                    }
                }

                @Override
                public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) throws Exception {
                    double dataSpeed = LogUtils.calculateMegaBytesPerSecond(System.currentTimeMillis() - sendFileStartTime, progress);
                    logger.debug(
                        "Sending file: {}, progress: {} out of {} bytes, {} mbs, {}",
                        splitFile, progress, total, dataSpeed, connectionInfo);
                    executor.updateLiveness(appShuffleId.getAppId());
                }
            });
        }
    }
}
