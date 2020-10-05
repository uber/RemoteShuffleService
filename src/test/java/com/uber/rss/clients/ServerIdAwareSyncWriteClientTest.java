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
import com.uber.rss.exceptions.RssInvalidServerIdException;
import com.uber.rss.exceptions.RssInvalidServerVersionException;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.StreamServerTestUtils;
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
        return new Object[][] { { true, true }, { false, false } };
    }
    
    @Test(dataProvider = "data-provider")
    public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
            networkTimeoutMillis,
            finishUploadAck,
            usePooledConnection,
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

            writeClient.sendRecord(0, null);

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);

            writeClient.close();
            writeClient.close();
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider")
    public void closeClientAfterServerShutdown(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
            networkTimeoutMillis,
            finishUploadAck,
            usePooledConnection,
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

            writeClient.sendRecord(0, null);

            writeClient.finishUpload();

            StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));

            testServer1.shutdown();

            writeClient.close();
        } finally {
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_retryOnInvalidHostPort(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), "invalid_host_port:12345"),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
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

            writeClient.sendRecord(0, null);

            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));
            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 2);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider", expectedExceptions = {RssInvalidServerVersionException.class})
    public void writeAndReadRecords_retryOnInvalidRunningVersion(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail(testServer1.getServerId(), "invalid_version", String.format("localhost:%s", testServer1.getShufflePort())),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
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

            writeClient.sendRecord(0, null);

            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));
            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));

            writeClient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 2);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider", expectedExceptions = {RssInvalidServerIdException.class})
    public void writeAndReadRecords_invalidServerId(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (ServerIdAwareSyncWriteClient writeClient = new ServerIdAwareSyncWriteClient(
                new ServerDetail("invalid_server_id", testServer1.getRunningVersion(), String.format("localhost:%s", testServer1.getShufflePort())),
                networkTimeoutMillis,
                finishUploadAck,
                usePooledConnection,
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
