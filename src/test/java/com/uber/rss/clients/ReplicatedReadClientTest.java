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

import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssEndOfStreamException;
import com.uber.rss.exceptions.RssInconsistentReplicaException;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ReplicatedReadClientTest {
  private static final Logger logger = LoggerFactory.getLogger(ReplicatedReadClientTest.class);

  private final int numLargeAmountRecords = 3000000;

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][] { { false, 0 }, { true, 0 }, { true, 10 } };
  }

  @DataProvider(name = "data-provider-3")
  public Object[][] dataProviderMethod3() {
    return new Object[][] { { false, 0, 1, true }, { true, 0, 1000, false }, { true, 10, 100000, true }, { false, 10, 500000, true }, { true, 10, 500000, false } };
  }

  @Test(dataProvider = "data-provider")
  public void oneServer(boolean finishUploadAck, int readQueueSize) {
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
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT))) {
        readClient.connect();
        TaskByteArrayDataBlock record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

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
  public void twoServers(boolean finishUploadAck, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail1, serverDetail2));

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
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT))) {
        readClient.connect();
        TaskByteArrayDataBlock record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

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
    }
  }

  @Test(dataProvider = "data-provider")
  public void firstServerDownBeforeReadClientInitialize(boolean finishUploadAck, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail1, serverDetail2));

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
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        writeClient.sendRecord(1, null);
        writeClient.sendRecord(1,
            ByteBuffer.wrap(new byte[0]));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
        writeClient.sendRecord(1,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      testServer1.shutdown();

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT))) {
        readClient.connect();
        TaskByteArrayDataBlock record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNotNull(record);

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
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider-3")
  public void firstServerDownAfterReadClientInitialize(boolean finishUploadAck, int readQueueSize, int numRecords, boolean checkDataConsistency) {
    logger.info(String.format("Running test firstServerDownAfterReadClientInitialize, %s", readQueueSize));

    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    logger.info(String.format("Test server 1 port: %s, test server 1 port: %s", testServer1.getShufflePort(), testServer2.getShufflePort()));

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail1, serverDetail2));

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
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        // Send out enough data to shuffle server, so read client socket will only read part of them during connect. This
        // is to help to trigger error on following readRecord() operation on first server. So we could test the client
        // switching to read second server.
        for (int i = 0; i < numRecords; i++) {
          writeClient.sendRecord(1,
              ByteBuffer.wrap(("value" + i).getBytes(StandardCharsets.UTF_8)));
        }

        writeClient.sendRecord(2,
            ByteBuffer.wrap(new byte[0]));

        writeClient.sendRecord(3,
            ByteBuffer.wrap("p3_value1".getBytes(StandardCharsets.UTF_8)));

        writeClient.finishUpload();
      }

      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT), checkDataConsistency)) {
        readClient.connect();

        for (int i = 0; i < numRecords; i++) {
          if (i == 1) {
            testServer1.shutdown();
          }

          TaskByteArrayDataBlock record = readClient.readRecord();
          Assert.assertNotNull(record);

          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value" + i);
        }

        Assert.assertNull(readClient.readRecord());

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 2
      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT), checkDataConsistency)) {
        readClient.connect();
        TaskByteArrayDataBlock record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(record.getValue(), new byte[0]);

        Assert.assertNull(readClient.readRecord());

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }

      // read partition 3
      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT), checkDataConsistency)) {
        readClient.connect();
        TaskByteArrayDataBlock record = readClient.readRecord();
        Assert.assertNotNull(record);

        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "p3_value1");

        Assert.assertNull(readClient.readRecord());

        long shuffleReadBytes = readClient.getShuffleReadBytes();
        Assert.assertTrue(shuffleReadBytes > 0);

        readClient.close();

        long shuffleReadBytes2 = readClient.getShuffleReadBytes();
        Assert.assertEquals(shuffleReadBytes2, shuffleReadBytes);
      }
    } finally {
      testServer2.shutdown();
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = {RssAggregateException.class, RssEndOfStreamException.class})
  public void twoServerDown(boolean finishUploadAck, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail1, serverDetail2));

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
        "user1",
        appTaskAttemptId.getAppId(),
        appTaskAttemptId.getAppAttempt(),
        new ShuffleWriteConfig()
    )) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

      // Send out enough data to shuffle server, so read client socket will only read part of them during connect. This
      // is to help to trigger error on following readRecord() operation on first server. So we could test the client
      // switching to read second server.
      for (int i = 0; i < numLargeAmountRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap(("value" + i).getBytes(StandardCharsets.UTF_8)));
      }

      writeClient.sendRecord(2,
          ByteBuffer.wrap(new byte[0]));

      writeClient.sendRecord(3,
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      writeClient.finishUpload();
    }

    // use short timeout to make the test fails fast (should get expected exception)
    int timeoutMillis = 200;

    AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
    try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, timeoutMillis, readQueueSize,"user1", appShufflePartitionId,
        new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT))) {
      readClient.connect();

      // shutdown all servers
      testServer1.shutdown();
      testServer2.shutdown();

      for (int i = 0; i < numLargeAmountRecords; i++) {
        Assert.assertNotNull(readClient.readRecord());
      }

      Assert.assertNull(readClient.readRecord());
    }
  }

  @Test(dataProvider = "data-provider", expectedExceptions = RssInconsistentReplicaException.class)
  public void inconsistentReplicaData(boolean finishUploadAck, int readQueueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    ServerDetail serverDetail1 = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
    ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
    ServerReplicationGroup serverReplicationGroup = new ServerReplicationGroup(Arrays.asList(serverDetail1, serverDetail2));

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      // write shuffle data to server1
      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          new ServerReplicationGroup(Arrays.asList(serverDetail1)),
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        // Send out enough data to shuffle server, so read client socket will only read part of them during connect. This
        // is to help to trigger error on following readRecord() operation on first server. So we could test the client
        // switching to read second server.
        for (int i = 0; i < numLargeAmountRecords; i++) {
          writeClient.sendRecord(1,
              ByteBuffer.wrap(("server1_value" + i).getBytes(StandardCharsets.UTF_8)));
        }

        writeClient.finishUpload();
      }

      // write different shuffle data to server2
      try (ReplicatedWriteClient writeClient = new ReplicatedWriteClient(
          new ServerReplicationGroup(Arrays.asList(serverDetail2)),
          TestConstants.NETWORK_TIMEOUT,
          finishUploadAck,
          false,
          "user1",
          appTaskAttemptId.getAppId(),
          appTaskAttemptId.getAppAttempt(),
          new ShuffleWriteConfig()
      )) {
        writeClient.connect();
        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

        // Send out enough data to shuffle server, so read client socket will only read part of them during connect. This
        // is to help to trigger error on following readRecord() operation on first server. So we could test the client
        // switching to read second server.
        for (int i = 0; i < numLargeAmountRecords; i++) {
          writeClient.sendRecord(1,
              ByteBuffer.wrap(("server2_value" + i).getBytes(StandardCharsets.UTF_8)));
        }

        writeClient.finishUpload();
      }

      // connect to server group, shutdown server 1, read shuffle data, will hit RssInconsistentReplicaException
      AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
      try (ReplicatedReadClient readClient = new ReplicatedReadClient(serverReplicationGroup, TestConstants.NETWORK_TIMEOUT, readQueueSize,"user1", appShufflePartitionId,
          new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT))) {
        readClient.connect();

        testServer1.shutdown();

        for (int i = 0; i < numLargeAmountRecords; i++) {
          if (i%1000 == 0) {
            logger.info("Reading record: " + i);
          }
          TaskByteArrayDataBlock record = readClient.readRecord();
          Assert.assertNotNull(record);

        }
      }
    } finally {
      testServer2.shutdown();
    }
  }
}
