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

package com.uber.rss.testutil;

import com.uber.rss.clients.PlainRecordSocketReadClient;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.clients.SingleServerReadClient;
import com.uber.rss.clients.TaskByteArrayDataBlock;
import com.uber.rss.exceptions.RssException;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StreamServerTestUtils {

    public static List<String> createTempDirectories(int count) {
        List<String> dirs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            try {
                dirs.add(Files.createTempDirectory("RssShuffleManagerTest").toString());
            } catch (IOException e) {
                throw new RssException("Failed to create temp directory", e);
            }
        }
        return dirs;
    }

    public static List<TaskByteArrayDataBlock> readAllRecords2(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds) {
        return readAllRecords2(port, appShuffleId, partitionId, latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_TIMEOUT);
    }

    public static List<TaskByteArrayDataBlock> readAllRecords(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds, int dataAvailableWaitTime) {
        SingleServerReadClient readClient = new PlainRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableWaitTime);

        try {
            AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
                    appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), partitionId);

            readClient.connect();

            List<TaskByteArrayDataBlock> result = new ArrayList<>();

            TaskByteArrayDataBlock record = readClient.readRecord();
            while (record != null) {
                result.add(record);
                record = readClient.readRecord();
            }
            return result;
        } finally {
            readClient.close();
        }
    }

    public static List<TaskByteArrayDataBlock> readAllRecords2(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds, int dataAvailableWaitTime) {
        SingleServerReadClient readClient = null;
        readClient = new PlainRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableWaitTime);

        try {
            AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
                appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), partitionId);

            readClient.connect();

            List<TaskByteArrayDataBlock> result = new ArrayList<>();

            TaskByteArrayDataBlock record = readClient.readRecord();
            while (record != null) {
                result.add(record);
                record = readClient.readRecord();
            }
            return result;
        } finally {
            readClient.close();
        }
    }

    public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId, Collection<Integer> partitionIds, Collection<Long> latestTaskAttemptIds) {
        for (Integer p: partitionIds) {
            waitTillDataAvailable(port, appShuffleId, p, latestTaskAttemptIds);
        }
    }

    public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds) {
        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
            appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), partitionId);
        SingleServerReadClient readClient = readClient = new PlainRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
        try {
            readClient.connect();
            readClient.readRecord();
        } finally {
            readClient.close();
        }
    }
}
