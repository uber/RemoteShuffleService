package com.uber.rss.testutil;

import com.uber.rss.clients.CompressedRecordSocketReadClient;
import com.uber.rss.clients.PlainRecordSocketReadClient;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.clients.SingleServerReadClient;
import com.uber.rss.clients.RecordKeyValuePair;
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

    public static List<RecordKeyValuePair> readAllRecords(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds) {
        return readAllRecords(port, appShuffleId, partitionId, latestTaskAttemptIds, false);
    }

    public static List<RecordKeyValuePair> readAllRecords(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds, boolean dataCompressed) {
        return readAllRecords(port, appShuffleId, partitionId, latestTaskAttemptIds, dataCompressed, TestConstants.DATA_AVAILABLE_TIMEOUT);
    }

    public static List<RecordKeyValuePair> readAllRecords(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds, boolean dataCompressed, int dataAvailableWaitTime) {
        SingleServerReadClient readClient = null;
        if (dataCompressed) {
            readClient = new CompressedRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableWaitTime);
        } else {
            readClient = new PlainRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableWaitTime);
        }

        try {
            AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
                    appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), partitionId);

            readClient.connect();
            
            List<RecordKeyValuePair> result = new ArrayList<>();

            RecordKeyValuePair record = readClient.readRecord();
            while (record != null) {
                result.add(record);
                record = readClient.readRecord();
            }
            return result;
        } finally {
            readClient.close();
        }
    }

    public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId, Collection<Integer> partitionIds, Collection<Long> latestTaskAttemptIds, boolean dataCompressed) {
        for (Integer p: partitionIds) {
            waitTillDataAvailable(port, appShuffleId, p, latestTaskAttemptIds, dataCompressed);
        }
    }

    public static void waitTillDataAvailable(int port, AppShuffleId appShuffleId, int partitionId, Collection<Long> latestTaskAttemptIds, boolean dataCompressed) {
        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
            appShuffleId.getAppId(), appShuffleId.getAppAttempt(), appShuffleId.getShuffleId(), partitionId);
        SingleServerReadClient readClient = null;
        if (dataCompressed) {
            readClient = new CompressedRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
        } else {
            readClient = new PlainRecordSocketReadClient("localhost", port, TestConstants.NETWORK_TIMEOUT, "user1", new AppShufflePartitionId(appShuffleId, partitionId), latestTaskAttemptIds, TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
        }
        try {
            readClient.connect();
            readClient.readRecord();
        } finally {
            readClient.close();
        }
    }
}
