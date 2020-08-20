package com.uber.rss;

import com.uber.rss.clients.CompressedRecordSyncWriteClient;
import com.uber.rss.clients.SingleServerWriteClient;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.execution.LocalFileStateStore;
import com.uber.rss.metadata.InMemoryServiceRegistry;
import com.uber.rss.metadata.ServiceRegistry;
import com.uber.rss.storage.ShuffleFileStorage;
import com.uber.rss.storage.ShuffleFileUtils;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.RetryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StreamServerCleanupTest {
    @Test
    public void cleanupExpiredApplicationFiles() throws Exception {
        long appRetentionMillis = 1000;
        
        StreamServerConfig config = new StreamServerConfig();
        config.setShufflePort(0);
        config.setHttpPort(0);
        config.setJFxDebugProfilerEnable(false);
        config.setAppMemoryRetentionMillis(appRetentionMillis);
        config.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
        config.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);

        ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
        TestStreamServer testServer = TestStreamServer.createRunningServer(config, serviceRegistry);

        String rootDir = testServer.getRootDir();
        
        AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (SingleServerWriteClient writeClient = new CompressedRecordSyncWriteClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt(), TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG)) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId1, 1, 20);

            writeClient.sendRecord(1, null, null);

            writeClient.sendRecord(2,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));

            writeClient.sendRecord(3,
                    ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

            writeClient.finishUpload();

            StreamServerTestUtils.waitTillDataAvailable(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), Arrays.asList(1, 2, 3), Arrays.asList(appTaskAttemptId1.getTaskAttemptId()), false);
            
            // The shuffle files should be still there. Those files will only be deleted when they expire and 
            // there is new application shuffle upload.
            ShuffleFileStorage shuffleFileStorage = new ShuffleFileStorage();
            List<String> files = shuffleFileStorage.listAllFiles(rootDir).stream()
                .filter(t -> !Paths.get(t).getFileName().toString().startsWith(LocalFileStateStore.STATE_FILE_PREFIX))
                .collect(Collectors.toList());
            Assert.assertEquals(files.size(), 3 * TestConstants.SHUFFLE_WRITE_CONFIG.getNumSplits());
            Assert.assertTrue(shuffleFileStorage.exists(ShuffleFileUtils.getAppShuffleDir(rootDir, appTaskAttemptId1.getAppId())));
            
            // Start a new application shuffle upload to trigger deleting the old files

            Thread.sleep(appRetentionMillis);

            AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app2", "exec1", 1, 2, 0L);

            try (SingleServerWriteClient writeClient2 = new CompressedRecordSyncWriteClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appTaskAttemptId2.getAppId(), appTaskAttemptId2.getAppAttempt(), TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG)) {
                writeClient2.connect();
                writeClient2.startUpload(appTaskAttemptId2, 1, 20);
            }
            // Check there should no files
            boolean hasNoAppDir = RetryUtils.retryUntilTrue(
                    100, 
                    10000, 
                    ()->shuffleFileStorage.exists(ShuffleFileUtils.getAppShuffleDir(rootDir, appTaskAttemptId1.getAppId())));
            Assert.assertTrue(hasNoAppDir);
        } finally {
            testServer.shutdown();
        }
    }
    
}
