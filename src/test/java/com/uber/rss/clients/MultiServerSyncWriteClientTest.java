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
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.ServerHostAndPort;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MultiServerSyncWriteClientTest {

    int networkTimeoutMillis = 5000;
    int maxTryingMillis = 10000;

    @DataProvider(name = "data-provider")
    public Object[][] dataProviderMethod() {
        return new Object[][] { { false, true}, { true, false } };
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_noRecord(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails = Arrays.asList(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString()));

        try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
                Arrays.asList(new ServerReplicationGroup(serverDetails)),
                networkTimeoutMillis,
                maxTryingMillis,
            finishUploadAck,
            usePooledConnection,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.finishUpload();

            List<TaskByteArrayDataBlock> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
        }
    }

    @Test(dataProvider = "data-provider")
    public void closeClientMultiTimes(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails = Arrays.asList(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString()));

        try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(serverDetails)),
            networkTimeoutMillis,
            maxTryingMillis,
            finishUploadAck,
            usePooledConnection,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null);

            writeClient.finishUpload();

            List<TaskByteArrayDataBlock> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
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
    public void writeAndReadRecords_twoServers(boolean finishUploadAck, boolean usePooledConnection) {
        TestStreamServer testServer1 = TestStreamServer.createRunningServer();
        TestStreamServer testServer2 = TestStreamServer.createRunningServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails1 = Arrays.asList(
            new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString()));
        List<ServerDetail> serverDetails2 = Arrays.asList(
            new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString()));

        try (MultiServerWriteClient writeClient = new MultiServerSyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(serverDetails1), new ServerReplicationGroup(serverDetails2)),
                networkTimeoutMillis,
                maxTryingMillis,
                finishUploadAck,
                usePooledConnection,
                "user1",
                appTaskAttemptId.getAppId(),
                appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null);

            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));

            for (int i = 0; i < 1000; i ++) {
                writeClient.sendRecord(2,
                    ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
                writeClient.sendRecord(3,
                    ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));
            }

            writeClient.finishUpload();

            List<TaskByteArrayDataBlock> records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            Assert.assertEquals(records.get(0).getValue(), new byte[0]);

            records = StreamServerTestUtils.readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            Assert.assertEquals(records.get(0).getValue(), new byte[0]);

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1000);

            Assert.assertEquals(records.get(0).getValue(), "value2".getBytes(StandardCharsets.UTF_8));

            Assert.assertEquals(records.get(records.size() - 1).getValue(), "value2".getBytes(StandardCharsets.UTF_8));

            records = StreamServerTestUtils.readAllRecords2(testServer2.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1000);

            Assert.assertEquals(records.get(0).getValue(), "value3".getBytes(StandardCharsets.UTF_8));

            Assert.assertEquals(records.get(records.size() - 1).getValue(), "value3".getBytes(StandardCharsets.UTF_8));

            records = StreamServerTestUtils.readAllRecords2(testServer1.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 4, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer1.shutdown();
            testServer2.shutdown();
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_twoServersPerPartition(boolean finishUploadAck, boolean usePooledConnection) {
        int numTestServers = 5;
        List<TestStreamServer> testServers = new ArrayList<>();
        for (int i = 0; i < numTestServers; i++) {
            testServers.add(TestStreamServer.createRunningServer());
        }

        List<ServerDetail> serverDetails = testServers.stream()
            .map(t->new ServerDetail(t.getServerId(), t.getRunningVersion(), t.getShuffleConnectionString()))
            .collect(Collectors.toList());

        List<ServerDetail> group1 = new ArrayList<>();
        List<ServerDetail> group2 = new ArrayList<>();
        for (int i = 0; i < serverDetails.size(); i++) {
            if (i % 2 == 0) {
                group1.add(serverDetails.get(i));
            } else {
                group2.add(serverDetails.get(i));
            }
        }

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        int numServersPerPartition = 2;
        int numRecords = 100000;

        List<TaskByteArrayDataBlock> partition2WriteRecords = new ArrayList<>();
        List<TaskByteArrayDataBlock> partition3WriteRecords = new ArrayList<>();

        try (MultiServerSyncWriteClient writeClient = new MultiServerSyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
            numServersPerPartition,
            networkTimeoutMillis,
            maxTryingMillis,
            null,
            finishUploadAck,
            usePooledConnection,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null);

            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));
            writeClient.sendRecord(1,
                ByteBuffer.wrap(new byte[0]));

            for (int i = 0; i < numRecords; i ++) {
                TaskByteArrayDataBlock partition2Record = new TaskByteArrayDataBlock(
                    ("value2_" + i).getBytes(StandardCharsets.UTF_8),
                    appTaskAttemptId.getTaskAttemptId());
                writeClient.sendRecord(2,
                    ByteBuffer.wrap(partition2Record.getValue()));
                partition2WriteRecords.add(partition2Record);

                TaskByteArrayDataBlock partition3Record = new TaskByteArrayDataBlock(
                    ("value33333333333333333333333333333333_" + i).getBytes(StandardCharsets.UTF_8),
                    appTaskAttemptId.getTaskAttemptId());
                writeClient.sendRecord(3,
                    ByteBuffer.wrap(partition3Record.getValue()));
                partition3WriteRecords.add(partition3Record);
            }

            writeClient.finishUpload();

            List<TaskByteArrayDataBlock> readRecords = new ArrayList<>();

            // Read partition 0 from first sever in replication group 1
            List<TaskByteArrayDataBlock> records;
            int port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Read partition 0 from last server in replication group 2
            port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Verify records for partition 0
            Assert.assertEquals(readRecords.size(), 1);
            Assert.assertEquals(readRecords.get(0).getValue(), new byte[0]);

            readRecords = new ArrayList<>();

            // Read partition 1 from first sever in replication group 1
            port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Read partition 1 from last server in replication group 2
            port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Verify records for partition 1
            Assert.assertEquals(readRecords.size(), 2);
            Assert.assertEquals(readRecords.get(0).getValue(), new byte[0]);
            Assert.assertEquals(readRecords.get(1).getValue(), new byte[0]);

            readRecords = new ArrayList<>();

            // Read partition 2 from first sever in replication group 1
            port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Read partition 2 from last server in replication group 2
            port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Verify records for partition 2
            Assert.assertEquals(readRecords.size(), numRecords);
            Assert.assertEquals(readRecords.size(), partition2WriteRecords.size());
            Assert.assertEquals(new HashSet<>(readRecords), new HashSet<>(partition2WriteRecords));

            readRecords = new ArrayList<>();

            // Read partition 3 from first sever in replication group 1
            port = ServerHostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Read partition 3 from last server in replication group 2
            port = ServerHostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords2(port, appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            readRecords.addAll(records);

            // Verify records for partition 3
            Assert.assertEquals(readRecords.size(), numRecords);
            Assert.assertEquals(readRecords.size(), partition3WriteRecords.size());
            Assert.assertEquals(new HashSet<>(readRecords), new HashSet<>(partition3WriteRecords));
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }
}
