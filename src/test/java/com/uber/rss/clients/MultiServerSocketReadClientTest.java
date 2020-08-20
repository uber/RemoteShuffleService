package com.uber.rss.clients;

import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssMissingShuffleWriteConfigException;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.exceptions.RssShuffleDataNotAvailableException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class MultiServerSocketReadClientTest {

  private boolean checkShuffleReplicaConsistency = true;

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][] { { false, 0, 0 }, { true, TestConstants.COMPRESSION_BUFFER_SIZE, 0 }, { true, TestConstants.COMPRESSION_BUFFER_SIZE, 10 } };
  }

  @Test(dataProvider = "data-provider")
  public void oneServer(boolean finishUploadAck, int compressionBufferSize, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertNull(record.getKey());
        Assert.assertNull(record.getValue());

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getKey(), new byte[0]);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void fourServers_onlySecondServerHasData(boolean finishUploadAck, int compressionBufferSize, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer();
    TestStreamServer testServer4 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 = new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 = new ServerReplicationGroup(Arrays.asList(serverDetail2));

    ServerDetail serverDetail3 = new ServerDetail(testServer3.getServerId(), testServer3.getRunningVersion(), testServer3.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup3 = new ServerReplicationGroup(Arrays.asList(serverDetail3));

    ServerDetail serverDetail4 = new ServerDetail(testServer4.getServerId(), testServer4.getRunningVersion(), testServer4.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup4 = new ServerReplicationGroup(Arrays.asList(serverDetail4));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup3,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup4,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3, serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertNull(record.getKey());
        Assert.assertNull(record.getValue());

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getKey(), new byte[0]);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
      testServer3.shutdown();
      testServer4.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void fourServers_twoServersHaveData(boolean finishUploadAck, int compressionBufferSize, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer();
    TestStreamServer testServer4 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 = new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 = new ServerReplicationGroup(Arrays.asList(serverDetail2));

    ServerDetail serverDetail3 = new ServerDetail(testServer3.getServerId(), testServer3.getRunningVersion(), testServer3.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup3 = new ServerReplicationGroup(Arrays.asList(serverDetail3));

    ServerDetail serverDetail4 = new ServerDetail(testServer4.getServerId(), testServer4.getRunningVersion(), testServer4.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup4 = new ServerReplicationGroup(Arrays.asList(serverDetail4));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup3,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1,
            ByteBuffer.wrap("key2".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(1,
            ByteBuffer.wrap("key3".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup4,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();
      }

      // read partition 1
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3, serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertNull(record.getKey());
        Assert.assertNull(record.getValue());

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getKey(), new byte[0]);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key2");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value2");

        record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key3");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value3");

        record = readClient.readRecord();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 2
      AppShufflePartitionId appShufflePartitionId2 = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3, serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId2,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getKey(), new byte[0]);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 3
      AppShufflePartitionId appShufflePartitionId3 = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3, serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,"user1",
          appShufflePartitionId3,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNull(record);

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 4 (no data)
      AppShufflePartitionId appShufflePartitionId4 = new AppShufflePartitionId(appId, appAttempt, shuffleId, 4);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2, serverReplicationGroup3, serverReplicationGroup4),
          TestConstants.NETWORK_TIMEOUT,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId4,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT),
          checkShuffleReplicaConsistency)) {
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.connect();
        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNull(record);

        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);

        readClient.close();

        Assert.assertEquals(readClient.getShuffleReadBytes(), 0);
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
      testServer3.shutdown();
      testServer4.shutdown();
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = {RssMissingShuffleWriteConfigException.class, RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class, RssAggregateException.class})
  public void twoServers_firstServerHasNoUpload(boolean finishUploadAck, int compressionBufferSize, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 = new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 = new ServerReplicationGroup(Arrays.asList(serverDetail2));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup2,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      // use short wait time to make the test finish fast
      int timeoutMillis = 500;
      int dataAvailableMaxWaitTime = 500;
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2),
          timeoutMillis,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableMaxWaitTime),
          checkShuffleReplicaConsistency)) {
        readClient.connect();
        readClient.readRecord();
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = {RssMissingShuffleWriteConfigException.class, RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class, RssAggregateException.class})
  public void twoServers_secondServerHasNoUpload(boolean finishUploadAck, int compressionBufferSize, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup1 = new ServerReplicationGroup(Arrays.asList(serverDetail1));

    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup2 = new ServerReplicationGroup(Arrays.asList(serverDetail2));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          serverReplicationGroup1,
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          compressionBufferSize,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]),
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      // use short wait time to make the test finish fast
      int timeoutMillis = 500;
      int dataAvailableMaxWaitTime = 500;
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (MultiServerSocketReadClient readClient = new MultiServerSocketReadClient(Arrays.asList(serverReplicationGroup1, serverReplicationGroup2),
          timeoutMillis,
          compressionBufferSize > 0,
          readQueueSize,
          "user1",
          appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, dataAvailableMaxWaitTime),
          checkShuffleReplicaConsistency)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        while (record != null) {
          record = readClient.readRecord();
        }
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }
  }
}
