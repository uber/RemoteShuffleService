package com.uber.rss.clients;

import com.google.common.net.HostAndPort;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
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

public class MultiServerAsyncWriteClientTest {

    @DataProvider(name = "data-provider")
    public Object[][] dataProviderMethod2() {
        return new Object[][] { { 1, false, true, 0, 1 }, { 2, true, false, TestConstants.COMPRESSION_BUFFER_SIZE, 10 } };
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_noRecord(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        List<TestStreamServer> testServers = new ArrayList<>();
        for (int i = 0; i < numTestServers; i++) {
            testServers.add(TestStreamServer.createRunningServer());
        }

        int numWriteThreads = 2;

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails = testServers.stream()
            .map(t->new ServerDetail(t.getServerId(), t.getRunningVersion(), t.getShuffleConnectionString()))
            .collect(Collectors.toList());

        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
                Arrays.asList(new ServerReplicationGroup(serverDetails)),
                TestConstants.NETWORK_TIMEOUT,
                TestConstants.NETWORK_TIMEOUT,
                finishUploadAck,
                usePooledConnection,
                compressionBufferSize,
                writeQueueSize,
                numWriteThreads,
                "user1",
                appTaskAttemptId.getAppId(),
                appTaskAttemptId.getAppAttempt(),
                new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.finishUpload();

            for (TestStreamServer testServer : testServers) {
                List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 0);

                records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 0);
            }
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }

    @Test(dataProvider = "data-provider", expectedExceptions = {RssNetworkException.class})
    public void connectInvalidServer(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        List<TestStreamServer> testServers = new ArrayList<>();
        for (int i = 0; i < numTestServers; i++) {
            testServers.add(TestStreamServer.createRunningServer());
        }

        int numWriteThreads = 2;

        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails = Arrays.asList(new ServerDetail("server1", "12345", "invalid_server:80"));

        int networkTimeout = 1000;
        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(serverDetails)),
            networkTimeout,
            networkTimeout,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            writeQueueSize,
            numWriteThreads,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
        )) {
            writeClient.connect();
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }

    @Test(dataProvider = "data-provider")
    public void closeClientMultiTimes(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        List<TestStreamServer> testServers = new ArrayList<>();
        for (int i = 0; i < numTestServers; i++) {
            testServers.add(TestStreamServer.createRunningServer());
        }

        int numWriteThreads = 2;

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        List<ServerDetail> serverDetails = testServers.stream()
            .map(t->new ServerDetail(t.getServerId(), t.getRunningVersion(), t.getShuffleConnectionString()))
            .collect(Collectors.toList());

        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(serverDetails)),
            TestConstants.NETWORK_TIMEOUT,
            TestConstants.NETWORK_TIMEOUT,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            writeQueueSize,
            numWriteThreads,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
        )) {
            writeClient.connect();
            writeClient.startUpload(appTaskAttemptId, numMaps, 20);

            writeClient.sendRecord(0, null, null);

            writeClient.finishUpload();

            writeClient.close();
            writeClient.close();
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        List<TestStreamServer> testServers = new ArrayList<>();
        for (int i = 0; i < numTestServers; i++) {
            testServers.add(TestStreamServer.createRunningServer());
        }

        List<ServerDetail> serverDetails = testServers.stream()
            .map(t->new ServerDetail(t.getServerId(), t.getRunningVersion(), t.getShuffleConnectionString()))
            .collect(Collectors.toList());

        int numWriteThreads = 2;

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(serverDetails)),
            TestConstants.NETWORK_TIMEOUT,
            TestConstants.NETWORK_TIMEOUT,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            writeQueueSize,
            numWriteThreads,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
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

            writeClient.sendRecord(2,
                    ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
            writeClient.sendRecord(2,
                    ByteBuffer.wrap("key2".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
            writeClient.sendRecord(2,
                    ByteBuffer.wrap("key3".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));

            writeClient.finishUpload();

            for (TestStreamServer testServer : testServers) {
                List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 1);
                Assert.assertEquals(records.get(0).getKey(), null);
                Assert.assertEquals(records.get(0).getValue(), null);

                records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 2);
                Assert.assertEquals(records.get(0).getKey(), new byte[0]);
                Assert.assertEquals(records.get(0).getValue(), new byte[0]);
                Assert.assertEquals(records.get(1).getKey(), new byte[0]);
                Assert.assertEquals(records.get(1).getValue(), new byte[0]);

                records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 3);
                Assert.assertEquals(records.get(0).getKey(), "key1".getBytes(StandardCharsets.UTF_8));
                Assert.assertEquals(records.get(0).getValue(), "value1".getBytes(StandardCharsets.UTF_8));
                Assert.assertEquals(records.get(1).getKey(), "key2".getBytes(StandardCharsets.UTF_8));
                Assert.assertEquals(records.get(1).getValue(), "value2".getBytes(StandardCharsets.UTF_8));
                Assert.assertEquals(records.get(2).getKey(), "key3".getBytes(StandardCharsets.UTF_8));
                Assert.assertEquals(records.get(2).getValue(), "value3".getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_twoServerGroups(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        if (numTestServers <= 1) {
            return;
        }

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

        int numWriteThreads = 2;

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        int numRecords = 100000;

        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
            TestConstants.NETWORK_TIMEOUT,
            TestConstants.NETWORK_TIMEOUT,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            writeQueueSize,
            numWriteThreads,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
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

            for (int i = 0; i < numRecords; i ++) {
                writeClient.sendRecord(2,
                    ByteBuffer.wrap(("key2_" + i).getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap(("value2_" + i).getBytes(StandardCharsets.UTF_8)));
                writeClient.sendRecord(3,
                    ByteBuffer.wrap(("key3_" + i).getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap(("value3_" + i).getBytes(StandardCharsets.UTF_8)));
            }

            writeClient.finishUpload();

            // Read from server group 1
            for (ServerDetail serverDetail: group1) {
                List<RecordKeyValuePair> records;
                int port = HostAndPort.fromString(serverDetail.getConnectionString()).getPort();

                records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 1);
                Assert.assertEquals(records.get(0).getKey(), null);
                Assert.assertEquals(records.get(0).getValue(), null);

                records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), numRecords);
                for (int i = 0; i < numRecords; i ++) {
                    Assert.assertEquals(new String(records.get(i).getKey(), StandardCharsets.UTF_8), "key2_" + i);
                    Assert.assertEquals(new String(records.get(i).getValue(), StandardCharsets.UTF_8), "value2_" + i);
                }
            }

            // Read from server group 2
            for (ServerDetail serverDetail: group2) {
                List<RecordKeyValuePair> records;
                int port = HostAndPort.fromString(serverDetail.getConnectionString()).getPort();

                records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), 2);
                Assert.assertEquals(records.get(0).getKey(), new byte[0]);
                Assert.assertEquals(records.get(0).getValue(), new byte[0]);
                Assert.assertEquals(records.get(1).getKey(), new byte[0]);
                Assert.assertEquals(records.get(1).getValue(), new byte[0]);

                records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
                Assert.assertEquals(records.size(), numRecords);
                for (int i = 0; i < numRecords; i ++) {
                    Assert.assertEquals(new String(records.get(i).getKey(), StandardCharsets.UTF_8), "key3_" + i);
                    Assert.assertEquals(new String(records.get(i).getValue(), StandardCharsets.UTF_8), "value3_" + i);
                }
            }
        } finally {
            testServers.forEach(t->t.shutdown());
        }
    }

    @Test(dataProvider = "data-provider")
    public void writeAndReadRecords_twoServersPerPartition(int numTestServers, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, int writeQueueSize) {
        if (numTestServers <= 1) {
            return;
        }

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

        int numWriteThreads = 2;

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        int numServersPerPartition = 2;
        int numRecords = 100000;

        List<RecordKeyValuePair> partition2WriteRecords = new ArrayList<>();
        List<RecordKeyValuePair> partition3WriteRecords = new ArrayList<>();

        try (MultiServerAsyncWriteClient writeClient = new MultiServerAsyncWriteClient(
            Arrays.asList(new ServerReplicationGroup(group1), new ServerReplicationGroup(group2)),
            numServersPerPartition,
            TestConstants.NETWORK_TIMEOUT,
            TestConstants.NETWORK_TIMEOUT,
            null,
            finishUploadAck,
            usePooledConnection,
            compressionBufferSize,
            writeQueueSize,
            numWriteThreads,
            "user1",
            appTaskAttemptId.getAppId(),
            appTaskAttemptId.getAppAttempt(),
            new ShuffleWriteConfig()
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

            for (int i = 0; i < numRecords; i ++) {
                RecordKeyValuePair partition2Record = new RecordKeyValuePair(
                    ("key2_" + i).getBytes(StandardCharsets.UTF_8),
                    ("value2_" + i).getBytes(StandardCharsets.UTF_8),
                    appTaskAttemptId.getTaskAttemptId());
                writeClient.sendRecord(2,
                    ByteBuffer.wrap(partition2Record.getKey()),
                    ByteBuffer.wrap(partition2Record.getValue()));
                partition2WriteRecords.add(partition2Record);

                RecordKeyValuePair partition3Record = new RecordKeyValuePair(
                    ("key33333333333333333333333333333333_" + i).getBytes(StandardCharsets.UTF_8),
                    ("value33333333333333333333333333333333_" + i).getBytes(StandardCharsets.UTF_8),
                    appTaskAttemptId.getTaskAttemptId());
                writeClient.sendRecord(3,
                    ByteBuffer.wrap(partition3Record.getKey()),
                    ByteBuffer.wrap(partition3Record.getValue()));
                partition3WriteRecords.add(partition3Record);
            }

            writeClient.finishUpload();

            List<RecordKeyValuePair> readRecords = new ArrayList<>();

            // Read partition 0 from first sever in replication group 1
            List<RecordKeyValuePair> records;
            int port = HostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Read partition 0 from last server in replication group 2
            port = HostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Verify records for partition 0
            Assert.assertEquals(readRecords.size(), 1);
            Assert.assertEquals(readRecords.get(0).getKey(), null);
            Assert.assertEquals(readRecords.get(0).getValue(), null);

            readRecords = new ArrayList<>();

            // Read partition 1 from first sever in replication group 1
            port = HostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Read partition 1 from last server in replication group 2
            port = HostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Verify records for partition 1
            Assert.assertEquals(readRecords.size(), 2);
            Assert.assertEquals(readRecords.get(0).getKey(), new byte[0]);
            Assert.assertEquals(readRecords.get(0).getValue(), new byte[0]);
            Assert.assertEquals(readRecords.get(1).getKey(), new byte[0]);
            Assert.assertEquals(readRecords.get(1).getValue(), new byte[0]);

            readRecords = new ArrayList<>();

            // Read partition 2 from first sever in replication group 1
            port = HostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Read partition 2 from last server in replication group 2
            port = HostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Verify records for partition 2
            Assert.assertEquals(readRecords.size(), numRecords);
            Assert.assertEquals(readRecords.size(), partition2WriteRecords.size());
            Assert.assertEquals(new HashSet<>(readRecords), new HashSet<>(partition2WriteRecords));

            readRecords = new ArrayList<>();

            // Read partition 3 from first sever in replication group 1
            port = HostAndPort.fromString(group1.get(0).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
            readRecords.addAll(records);

            // Read partition 3 from last server in replication group 2
            port = HostAndPort.fromString(group2.get(group2.size() - 1).getConnectionString()).getPort();
            records = StreamServerTestUtils.readAllRecords(port, appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), compressionBufferSize > 0);
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
