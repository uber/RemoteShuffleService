package com.uber.rss;

import com.uber.rss.clients.SingleServerWriteClient;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssFileCorruptedException;
import com.uber.rss.exceptions.RssFinishUploadException;
import com.uber.rss.metadata.InMemoryServiceRegistry;
import com.uber.rss.metadata.ServiceRegistry;
import com.uber.rss.testutil.ClientTestUtils;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.annotations.Test;

import java.util.Arrays;

public class StreamServerWritingTooMuchDataTest {

    @Test(expectedExceptions = {RssFileCorruptedException.class, RssFinishUploadException.class})
    public void writeDataExceedingAppMaxWriteBytes() {
        long appRetentionMillis = 1000;
        
        StreamServerConfig config = new StreamServerConfig();
        config.setShufflePort(0);
        config.setHttpPort(0);
        config.setJFxDebugProfilerEnable(false);
        config.setAppMemoryRetentionMillis(appRetentionMillis);
        config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
        config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);
        config.setAppMaxWriteBytes(4);

        ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
        TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (SingleServerWriteClient writeClient = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, 1, 20);

            writeClient.sendRecord(1, null, null);

            writeClient.finishUpload();

            StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        } finally {
            testServer.shutdown();
        }
    }

    @Test(expectedExceptions = {RssFileCorruptedException.class, RssFinishUploadException.class})
    public void writeDataExceedingAppMaxWriteBytesAndConnectAgain() {
        long appRetentionMillis = 1000;

        StreamServerConfig config = new StreamServerConfig();
        config.setShufflePort(0);
        config.setHttpPort(0);
        config.setJFxDebugProfilerEnable(false);
        config.setAppMemoryRetentionMillis(appRetentionMillis);
        config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
        config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);
        config.setAppMaxWriteBytes(4);

        ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
        TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

        try {
            AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

            try (SingleServerWriteClient writeClient = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
                writeClient.connect();
                writeClient.startUpload(appTaskAttemptId, 1, 20);

                writeClient.sendRecord(1, null, null);

                writeClient.finishUpload();
            }

            try (SingleServerWriteClient writeClient = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
                writeClient.connect();
                writeClient.startUpload(appTaskAttemptId, 1, 20);

                writeClient.sendRecord(1, null, null);

                writeClient.finishUpload();
            }

            StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        } finally {
            testServer.shutdown();
        }
    }
}
