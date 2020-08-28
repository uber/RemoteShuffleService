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

import com.uber.rss.common.MemoryMonitor;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssDuplicateAppTaskAttemptException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssMaxConnectionsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class UploadChannelManager {
    public static final int DEFAULT_MAX_CONNECTIONS = 40000;

    private static final Logger logger = LoggerFactory.getLogger(UploadChannelManager.class);
    
    // TODO monitor and clean up channels
    private ConcurrentHashMap<AppTaskAttemptId, UploadChannel> uploadChannels = new ConcurrentHashMap<>();

    private AtomicInteger numConnections = new AtomicInteger();

    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    
    public UploadChannelManager(ScheduledExecutorService scheduledExecutorService, int throttleMemoryPercentage, long maxUploadPauseMillis) {
        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.addLowMemoryListener(throttleMemoryPercentage, ()->{
            // TODO use a better way to choose channel to pause upload
            List<UploadChannel> channels = new ArrayList<>(uploadChannels.values());
            for (UploadChannel entry: channels) {
                entry.setAutoRead(false);
                logger.info(String.format("Disabled AutoRead for %s", entry.getAppTaskAttemptId()));
            }
        });
        
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                List<UploadChannel> channels = new ArrayList<>(uploadChannels.values());
                for (UploadChannel entry: channels) {
                    if (entry.needToRenableAutoRead(maxUploadPauseMillis)) {
                        entry.setAutoRead(true);
                        logger.info(String.format("Re-enabled AutoRead for %s", entry.getAppTaskAttemptId()));
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public void checkMaxConnections() throws RssMaxConnectionsException {
        int currentSize = uploadChannels.size();
        if (currentSize > maxConnections) {
            throw new RssMaxConnectionsException(currentSize, maxConnections, "Stream server connections exceed upper limit");
        }

        if (numConnections.get() > maxConnections) {
            throw new RssMaxConnectionsException(currentSize, maxConnections, "Stream server connections exceed upper limit");
        }
    }
    
    public void addUploadChannel(AppTaskAttemptId appTaskAttemptId, UploadChannel channel) {
        logger.info(String.format("Adding channel from channel manager for %s", appTaskAttemptId));
        UploadChannel oldValue = uploadChannels.put(appTaskAttemptId, channel);
        // TODO handle error (retry?) better
        if (oldValue != null) {
            throw new RssDuplicateAppTaskAttemptException(String.format(
                    "There is already upload channel for %s, something must be wrong", appTaskAttemptId));
        }
    }
    
    public void removeUploadChannel(AppTaskAttemptId appTaskAttemptId, UploadChannel channel) {
        logger.debug(String.format("Removing channel from channel manager for %s", appTaskAttemptId));
        UploadChannel oldValue = uploadChannels.remove(appTaskAttemptId);
        if (oldValue == null) {
            throw new RssInvalidStateException(String.format(
                    "There is no upload channel for %s, cannot remove channel", appTaskAttemptId));
        }
        if (channel != null && oldValue != channel) {
            throw new RssInvalidStateException(String.format(
                    "There is another upload channel for %s which is different from the channel to remove", appTaskAttemptId));
        }
    }

    public void incNumConnections() {
        numConnections.incrementAndGet();
    }

    public void decNumConnections() {
        numConnections.decrementAndGet();
    }
}
