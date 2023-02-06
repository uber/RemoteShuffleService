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

import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/***
 * This class uses a background thread to read records and put records into a blocking queue.
 * Then it returns records from the blocking queue to its caller.
 */
public class BlockingQueueReadClient implements BlockingSingleServerReadClient {
    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueReadClient.class);

    private final BlockingSingleServerReadClient delegate;

    private final BlockingQueue<TaskByteArrayDataBlock> recordQueue;
    private final long maxBlockingMillis;

    private volatile boolean stopped = false;

    public BlockingQueueReadClient(BlockingSingleServerReadClient delegate, int queueSize, long maxBlockingMillis) {
        this.delegate = delegate;

        this.recordQueue = new ArrayBlockingQueue<>(queueSize);
        logger.info("Created blocking queue with size: {}", queueSize);

        this.maxBlockingMillis = maxBlockingMillis;
    }

    @Override
    public DownloadServerVerboseInfo connect() {
        DownloadServerVerboseInfo serverVerboseInfo = delegate.connect();

        stopped = false;

        Thread thread = new Thread(() -> {
            logger.info("Started reading record in background thread");
            try {
                TaskByteArrayDataBlock record = delegate.readRecord();
                while (!stopped && record != null) {
                    recordQueue.put(record);
                    record = delegate.readRecord();
                }
                recordQueue.put(new EofRecordKeyValuePair());
            } catch (Throwable ex) {
                M3Stats.addException(ex, this.getClass().getSimpleName());
                logger.warn("Failed to read record, {}", delegate, ex);
                recordQueue.clear();
                recordQueue.add(new FailedFetchRecordKeyValuePair(ex));
            }
            logger.info("Finished reading record in background thread");
        });

        thread.start();

        return serverVerboseInfo;
    }

    @Override
    public TaskByteArrayDataBlock readRecord() {
        TaskByteArrayDataBlock record;
        try {
            record = recordQueue.poll(maxBlockingMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RssException("Interrupted when polling record from blocking queue", e);
        }

        if (record == null) {
            throw new RssInvalidStateException(String.format(
                    "Timed out to poll record from blocking queue after waiting %s milliseconds",
                    maxBlockingMillis));
        }

        if (record instanceof FailedFetchRecordKeyValuePair) {
            throw new RssException("Failed to read record", ((FailedFetchRecordKeyValuePair)record).getCause());
        }

        if (record instanceof EofRecordKeyValuePair) {
            return null;
        }

        return record;
    }

    @Override
    public long getShuffleReadBytes() {
        return delegate.getShuffleReadBytes();
    }

    @Override
    public void close() {
        stopped = true;
        delegate.close();
    }

    @Override
    public String toString() {
        return "BlockingQueueReadClient{" +
                "delegate=" + delegate +
                '}';
    }
}