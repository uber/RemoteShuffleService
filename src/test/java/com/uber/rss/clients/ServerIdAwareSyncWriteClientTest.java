package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssInvalidServerIdException;
import com.uber.rss.exceptions.RssInvalidServerVersionException;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ServerIdAwareSyncWriteClientTest {

    int networkTimeoutMillis = 5000;

    @DataProvider(name = "data-provider")
    public Object[][] dataProviderMethod() {
        return new Object[][] { { true, true, 0 }, { false, false, TestConstants.COMPRESSION_BUFFER_SIZE } };
    }
    
    @Test(dataProvider = "data-provider")
    public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
            networkTimeoutMillis,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig(),
            new ServerConnectionRefresher() {
                @Override
                public ServerDetail refreshConnection(ServerDetail serverDetail) {
                    throw new RuntimeException("Should not run into this method");
                }
            }
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null, null);

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 0);

            writeClient.close();
            writeClient.close();
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider")
    public void closeClientAfterServerShutdown(boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
            networkTimeoutMillis,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig(),
            new ServerConnectionRefresher() {
                @Override
                public ServerDetail refreshConnection(ServerDetail serverDetail) {
                    throw new RuntimeException("Should not run into this method");
                }
            }
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null, null);

            writeClient.finishUpload();

            StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);

            testServer1.shutdown();

            writeClient.close();
        } finally {
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_retryOnInvalidHostPort(boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), "invalid_host_port:12345"),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
                compressionBufferSize,
            "user1",
                appTaskAttemptId.getAppId(),
                appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig(),
            new ServerConnectionRefresher() {
                @Override
                public ServerDetail refreshConnection(ServerDetail serverDetail) {
                        return new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort()));
                    }
                }
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null, null);

            writeClient.sendRecord(1,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));
            writeClient.sendRecord(1,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 2);

            records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider", expectedExceptions = {RssInvalidServerVersionException.class})
    public void writeAndReadRecords_retryOnInvalidRunningVersion(boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail(testServer1.getServerId(), "invalid_version", String.format("localhost:%s", testServer1.getShufflePort())),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
                compressionBufferSize,
            "user1",
                appTaskAttemptId.getAppId(),
                appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig(),
            new ServerConnectionRefresher() {
                @Override
                public ServerDetail refreshConnection(ServerDetail serverDetail) {
                        return new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort()));
                    }
                }
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null, null);

            writeClient.sendRecord(1,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));
            writeClient.sendRecord(1,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 2);

            records = StreamServerTestUtils.readAllRecords(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider", expectedExceptions = {RssInvalidServerIdException.class})
    public void writeAndReadRecords_invalidServerId(boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail("invalid_server_id", testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
                compressionBufferSize,
            "user1",
                appTaskAttemptId.getAppId(),
                appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig(),
            new ServerConnectionRefresher() {
                @Override
                public ServerDetail refreshConnection(ServerDetail serverDetail) {
                        return new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort()));
                    }
                }
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);
        } finally {
            testServer1.shutdown();
        }
    }
}
