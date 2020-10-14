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

import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppMapId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.Compression;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssMaxConnectionsException;
import com.uber.rss.execution.ShuffleDataWrapper;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.util.NettyUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/***
 * This class handles messages for shuffle writer to upload data.
 */
public class UploadServerHandler {
    private static final Logger logger = LoggerFactory.getLogger(UploadServerHandler.class);

    private final ShuffleExecutor executor;
    private final UploadChannelManager channelManager;

    private final ConcurrentHashMap<Long, AppMapId> taskAttemptMap = new ConcurrentHashMap<>();

    private String connectionInfo;

    private int numPartitions;
    private ShuffleWriteConfig writeConfig;

    private final ConcurrentHashMap<Long, Boolean> taskAttemptUploadStarted = new ConcurrentHashMap<>();

    public UploadServerHandler(ShuffleExecutor executor, UploadChannelManager channelManager) {
        this.executor = executor;
        this.channelManager = channelManager;

        channelManager.incNumConnections();
    }

    public void initializeAppTaskAttempt(AppTaskAttemptId appTaskAttemptId, int numPartitions, ShuffleWriteConfig writeConfig, ChannelHandlerContext ctx) {
        initializeAppTaskAttemptImpl(appTaskAttemptId, numPartitions, writeConfig, ctx, null);
    }

    private void initializeAppTaskAttemptImpl(AppTaskAttemptId appTaskAttemptId, int numPartitions, ShuffleWriteConfig writeConfig, ChannelHandlerContext ctx, String networkCompressionCodecName) {
        this.connectionInfo = NettyUtils.getServerConnectionInfo(ctx.channel());

        this.numPartitions = numPartitions;
        this.writeConfig = writeConfig;

        AppMapId newAppMapIdValue = appTaskAttemptId.getAppMapId();
        AppMapId oldAppMapIdValue = this.taskAttemptMap.put(appTaskAttemptId.getTaskAttemptId(), newAppMapIdValue);
        if (oldAppMapIdValue != null) {
            if (!oldAppMapIdValue.equals(newAppMapIdValue)) {
                throw new RssInvalidStateException(String.format(
                    "There was already value %s with task attempt %s, but trying to set a different value %s",
                    oldAppMapIdValue, appTaskAttemptId.getTaskAttemptId(), newAppMapIdValue));
            }
        }

        if (networkCompressionCodecName != null && !networkCompressionCodecName.isEmpty()) {
            if (networkCompressionCodecName.equals(Compression.COMPRESSION_CODEC_LZ4)) {
                ctx.pipeline().addFirst(new Lz4FrameDecoder());
                logger.debug("Added LZ4 decoder, {}", connectionInfo);
            } else {
                logger.warn(String.format("Invalid compression codec %s, will fallback to not compression, %s", networkCompressionCodecName, connectionInfo));
            }
        }
    }

    public void checkMaxConnections() throws RssMaxConnectionsException {
        channelManager.checkMaxConnections();
    }

    public void updateLiveness(String appId) {
        executor.updateLiveness(appId);
    }

    public void checkAppMaxWriteBytes(String appId) {
        executor.checkAppMaxWriteBytes(appId);
    }

    public void onChannelInactive() {
        channelManager.decNumConnections();
    }

    public void writeRecord(com.uber.rss.messages.ShuffleDataWrapper shuffleDataWrapper) {
        AppMapId appMapId = getAppMapId(shuffleDataWrapper.getTaskAttemptId());

        lazyStartUpload(new AppTaskAttemptId(appMapId, shuffleDataWrapper.getTaskAttemptId()));

        if (shuffleDataWrapper.getPartitionId() < 0 || shuffleDataWrapper.getPartitionId() > numPartitions) {
            throw new RssInvalidDataException(String.format("Invalid partition: %s, %s", shuffleDataWrapper.getPartitionId(), connectionInfo));
        }

        executor.writeData(new ShuffleDataWrapper(
            appMapId.getAppShuffleId(), appMapId.getMapId(), shuffleDataWrapper.getTaskAttemptId(), shuffleDataWrapper.getPartitionId(), Unpooled.wrappedBuffer(shuffleDataWrapper.getBytes())));
    }

    public void finishUpload(long taskAttemptId) {
        AppMapId appMapId = getAppMapId(taskAttemptId);
        AppTaskAttemptId appTaskAttemptIdToFinishUpload = new AppTaskAttemptId(appMapId, taskAttemptId);
        finishUploadImpl(appTaskAttemptIdToFinishUpload);
    }

    private void finishUploadImpl(AppTaskAttemptId appTaskAttemptIdToFinishUpload) {
        lazyStartUpload(appTaskAttemptIdToFinishUpload);
        executor.addFinishUploadOperation(appTaskAttemptIdToFinishUpload.getAppShuffleId(), appTaskAttemptIdToFinishUpload.getTaskAttemptId());
        taskAttemptMap.remove(appTaskAttemptIdToFinishUpload.getTaskAttemptId());
        taskAttemptUploadStarted.remove(appTaskAttemptIdToFinishUpload.getTaskAttemptId());
    }

    // lazy initialize on executor when only upload the first record, so same map task
    // could retry connecting to the server without really start the upload
    private void lazyStartUpload(AppTaskAttemptId appTaskAttemptIdToStartUpload) {
        if (!taskAttemptUploadStarted.getOrDefault(appTaskAttemptIdToStartUpload.getTaskAttemptId(), false)) {
            executor.registerShuffle(appTaskAttemptIdToStartUpload.getAppShuffleId(), numPartitions, writeConfig);
            executor.startUpload(appTaskAttemptIdToStartUpload);

            taskAttemptUploadStarted.put(appTaskAttemptIdToStartUpload.getTaskAttemptId(), true);
        }
    }

    private AppMapId getAppMapId(long taskAttemptId) {
        AppMapId appMapId = taskAttemptMap.get(taskAttemptId);
        if (appMapId == null) {
            throw new RssInvalidStateException(String.format("Did not get app map id for task attempt %s, %s", taskAttemptId, connectionInfo));
        }
        return appMapId;
    }
}
