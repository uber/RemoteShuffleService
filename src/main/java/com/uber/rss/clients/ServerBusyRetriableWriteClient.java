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
import com.uber.rss.exceptions.RssServerBusyException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.messages.ConnectUploadResponse;
import com.uber.rss.metrics.WriteClientMetrics;
import com.uber.rss.metrics.WriteClientMetricsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/***
 * This is a write client which checks server busy response when connecting to server and will retry on server busy.
 */
public class ServerBusyRetriableWriteClient implements SingleServerWriteClient {
    private static final Logger logger = LoggerFactory.getLogger(ServerBusyRetriableWriteClient.class);
    
    private final Callable<SingleServerWriteClient> creator;
    private final long maxTryingMillis;
    private final String user;
    private final String appId;
    private final String appAttempt;
    private SingleServerWriteClient delegate;

    private WriteClientMetrics metrics = null;
    
    public ServerBusyRetriableWriteClient(Callable<SingleServerWriteClient> creator, long maxTryingMillis, String user, String appId, String appAttempt) {
        this.creator = creator;
        this.maxTryingMillis = maxTryingMillis;
        this.user = user;
        this.appId = appId;
        this.appAttempt = appAttempt;
    }

    @Override
    public ConnectUploadResponse connect() {
        metrics = new WriteClientMetrics(new WriteClientMetricsKey(
            this.getClass().getSimpleName(), user));

        long startTime = System.currentTimeMillis();
        long waitMillis = 1000;
        int maxTryTimes = 1000;

        int numTried = 0;
        ConnectUploadResponse serverVerboseInfo = null;
        for (; numTried < maxTryTimes; numTried++) {
            if (numTried > 0) {
                metrics.getNumRetries().inc(1);
            }

            try {
                delegate = creator.call();
            } catch (Exception e) {
                throw new RssException("Failed to create writer client", e);
            }

            try {
                serverVerboseInfo = delegate.connect();
                break;
            } catch (RssServerBusyException ex) {
                if (System.currentTimeMillis() - startTime < maxTryingMillis) {
                    logger.info(String.format("Server busy, will close current client and wait %s milliseconds to retry", waitMillis));
                    closeDelegate();
                    delegate = null;
                    try {
                        Thread.sleep(waitMillis);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting to retry", e);
                    }
                    // Reduce wait time to maxTryingMillis/10 if it becomes too large. This is to avoid very long wait.
                    waitMillis = Math.min(waitMillis * 2, maxTryingMillis / 10);
                } else {
                    throw ex;
                }
            }
        }

        if (delegate == null) {
            throw new RssException(String.format("Failed to create and initialize client after trying %s times", numTried));
        }

        return serverVerboseInfo;
    }

    @Override
    public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
        delegate.startUpload(appTaskAttemptId, numMaps, numPartitions);
    }

    @Override
    public void writeDataBlock(int partition, ByteBuffer value) {
        delegate.writeDataBlock(partition, value);
    }

    @Override
    public void finishUpload() {
        delegate.finishUpload();
    }

    @Override
    public long getShuffleWriteBytes() {
        return delegate.getShuffleWriteBytes();
    }

    @Override
    public void close() {
        if (metrics != null) {
            metrics.close();
        }

        closeDelegate();
    }

    @Override
    public String toString() {
        return "ServerBusyRetriableWriteClient{" +
            "delegate=" + delegate +
            '}';
    }

    private void closeDelegate() {
        try {
            delegate.close();
        } catch (Throwable e) {
            logger.warn(String.format("Failed to close delegate client %s", delegate), e);
        }
    }
}
