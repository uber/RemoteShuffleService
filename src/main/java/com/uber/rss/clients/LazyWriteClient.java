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

package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.util.RetryUtils;
import org.apache.spark.shuffle.RssOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * Connections have a idle timeout of
 * [[com.uber.rss.StreamServerConfig#DEFAULT_SERVER_SIDE_CONNECTION_IDLE_TIMEOUT_MILLIS]], so
 * if the connections are established at the start of map task and downstream operations in a stage
 * take a long time, these connections get timed out by the time mapper starts writing shuffle data.
 *
 * LazyWriteClient connects to the RSS servers just before the first batch of the data needs
 * to be sent to ensure that such timeouts do not happen.
 */
public final class LazyWriteClient implements MultiServerWriteClient {

    private final MultiServerWriteClient writeClient;
    private final AppTaskAttemptId mapInfo;
    private final int numMaps;
    private final int numPartitions;
    private final int pollInterval;
    private final long maxWaitMillis;

    private boolean connectedToWriteClient = false;

    private static final Logger logger =
            LoggerFactory.getLogger(HeartbeatSocketClient.class);

    public LazyWriteClient(MultiServerWriteClient writeClient, AppTaskAttemptId mapInfo,
                           int numMaps, int numPartitions, int pollInterval, long maxWaitMillis) {
        this.writeClient = writeClient;
        this.mapInfo = mapInfo;
        this.numMaps = numMaps;
        this.numPartitions = numPartitions;
        this.pollInterval = pollInterval;
        this.maxWaitMillis = maxWaitMillis;
        logger.info("Using lazy write client to connect to RSS servers");
    }


    /**
     * Without the LazyWriteClient, connection to clients is established at the very start of the mapper
     * task by calling RssShuffleManager.getWriter::writeClient.connect(). When writeClient in RssShuffleManager
     * is of type LazyWriteClient, this will be a no-op.
     *
     * RssShuffleWriter calls writeClient.writeDataBlock() for sending the shuffle data. So below in
     * LazyClient.writeDataBlock(), connection will be established if not already done by calling
     * lazyConnect() and laxyStartUpload().
     */
    @Override
    public void connect() {}

    @Override
    public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {}

    private void lazyStartUpload() {
        writeClient.startUpload(this.mapInfo, this.numMaps, this.numPartitions);
    }

    /**
     * There are retries in RssShuffleManager.getWriter() in case the chosen RSS server is not reachable.
     * These retries are not applicable for LazyWriteClient since the connection is established just before sending
     * first batch of the data. So we add similar retries here while establishing connection to RSS servers.
     */
    private void lazyConnect() {
        RetryUtils.retry(pollInterval, pollInterval * 10, maxWaitMillis, "create write client", () -> {
            writeClient.connect();
            return 0;
        });
    }

    @Override
    public void writeDataBlock(int partition, ByteBuffer value) {
        if (!connectedToWriteClient) {
            lazyConnect();
            lazyStartUpload();
            connectedToWriteClient = true;
        }
        writeClient.writeDataBlock(partition, value);
    }

    @Override
    public void finishUpload() {
        // Current design requires a finish acknowledgment to be sent to RSS servers even if the partitions are empty.
        // if the acknowledgment is not sent, reducer keeps waiting for shuffle data to be available and
        // then eventually fails.

        // TODO: Fix this behaviour. Empty partition is a pretty common occurrence. May be we can pass
        //  the empty partition information in dummy map status and reducer can ignore the empty partitions
        //  based on that.
        if (!connectedToWriteClient) {
            lazyConnect();
            lazyStartUpload();
            connectedToWriteClient = true;
        }
        writeClient.finishUpload();
    }

    @Override
    public long getShuffleWriteBytes() {
        return writeClient.getShuffleWriteBytes();
    }

    @Override
    public void close() {
        if (connectedToWriteClient) {
            writeClient.close();
        }
    }
}
