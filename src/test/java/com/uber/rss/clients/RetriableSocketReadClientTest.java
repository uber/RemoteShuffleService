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

import com.uber.rss.StreamServerConfig;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.function.Consumer;

public class RetriableSocketReadClientTest {

  @DataProvider(name = "data-provider")
  public Object[][] dataProviderMethod() {
    return new Object[][] { { false, 0, 0 }, { true, TestConstants.COMPRESSION_BUFFER_SIZE, 0 }, { true, TestConstants.COMPRESSION_BUFFER_SIZE, 10 } };
  }

  @Test(dataProvider = "data-provider")
  public void readRecords(boolean finishUploadAck, int compressionBufferSize, int queueSize) {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (RecordSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance().getOrCreateClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", "app1", appAttempt, compressionBufferSize, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
      ServerDetail serverDetail = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, serverId->serverDetail);
      ReadClientDataOptions readClientDataOptions = new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
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
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test(dataProvider = "data-provider")
  public void serverRestartAndRefreshServerConnection(boolean finishUploadAck, int compressionBufferSize, int queueSize) throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();
    Consumer<StreamServerConfig> configModifier = config -> config.setRootDirectory(rootDir);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(configModifier);
    TestStreamServer testServer2 = null;

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (RecordSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance().getOrCreateClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", "app1", appAttempt, compressionBufferSize, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
      ServerDetail serverDetail = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, serverId->serverDetail);
      ReadClientDataOptions readClientDataOptions = new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
      }

      testServer1.shutdown();
      testServer1 = null;

      testServer2 = TestStreamServer.createRunningServer(configModifier);

      ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions2 = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, serverId->serverDetail2);
      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions2,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
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
      }

      // test another retry scenario, first retry still uses old server connection and second retry uses new server connection
      ClientRetryOptions clientRetryOptions3 = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, new ServerConnectionRefresher() {
        private int count = 0;
        @Override
        public ServerDetail refreshConnection(ServerDetail serverDetail) {
          if (count++ == 0) {
            return serverDetail;
          } else {
            return serverDetail2;
          }
        }
      });

      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions3,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
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
      }
    } finally {
      if (testServer1 != null) {
        testServer1.shutdown();
      }
      if (testServer2 != null) {
        testServer2.shutdown();
      }
    }
  }

  @Test(dataProvider = "data-provider")
  public void serverRestartAndRefreshServerConnectionWithUnknownHostFirst(boolean finishUploadAck, int compressionBufferSize, int queueSize) throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();
    Consumer<StreamServerConfig> configModifier = config -> config.setRootDirectory(rootDir);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(configModifier);
    TestStreamServer testServer2 = null;

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (RecordSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance().getOrCreateClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", "app1", appAttempt, compressionBufferSize, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
      ServerDetail serverDetail = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, serverId->serverDetail);
      ReadClientDataOptions readClientDataOptions = new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
      }

      testServer1.shutdown();
      testServer1 = null;

      testServer2 = TestStreamServer.createRunningServer(configModifier);

      ServerDetail serverDetail2 = new ServerDetail(testServer2.getServerId(), testServer2.getRunningVersion(), testServer2.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions2 = new ClientRetryOptions(10, TestConstants.NETWORK_TIMEOUT, new ServerConnectionRefresher() {
        private int count = 0;
        @Override
        public ServerDetail refreshConnection(ServerDetail serverDetail) {
          if (count++ == 0) {
            return new ServerDetail(serverDetail.getServerId(), serverDetail.getRunningVersion(), "invalid_not_existing_server:9123");
          } else {
            return serverDetail2;
          }
        }
      });

      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          10,
          clientRetryOptions2,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
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
      }
    } finally {
      if (testServer1 != null) {
        testServer1.shutdown();
      }
      if (testServer2 != null) {
        testServer2.shutdown();
      }
    }
  }

  @Test(expectedExceptions = RssNetworkException.class)
  public void serverRestartWithOldServerConnection() throws IOException {
    boolean finishUploadAck = true;
    int compressionBufferSize = TestConstants.COMPRESSION_BUFFER_SIZE;
    int queueSize = 0;

    String rootDir = Files.createTempDirectory("StreamServer_").toString();
    Consumer<StreamServerConfig> configModifier = config -> config.setRootDirectory(rootDir);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(configModifier);
    TestStreamServer testServer2 = null;

    try {
      String appId = "app1";
      String appAttempt = "attempt1";
      int shuffleId = 1;
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 3;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      try (RecordSyncWriteClient writeClient = UnpooledWriteClientFactory.getInstance().getOrCreateClient(
          "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", "app1", appAttempt, compressionBufferSize, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
      ServerDetail serverDetail = new ServerDetail(testServer1.getServerId(), testServer1.getRunningVersion(), testServer1.getShuffleConnectionString());
      ClientRetryOptions clientRetryOptions = new ClientRetryOptions(10, 1000, serverId->serverDetail);
      ReadClientDataOptions readClientDataOptions = new ReadClientDataOptions(Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT);
      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
      }

      testServer1.shutdown();
      testServer1 = null;

      testServer2 = TestStreamServer.createRunningServer(configModifier);

      try (RetriableSocketReadClient readClient = new RetriableSocketReadClient(serverDetail,
          TestConstants.NETWORK_TIMEOUT,
          clientRetryOptions,
          compressionBufferSize > 0, queueSize,"user1", appShufflePartitionId,
          readClientDataOptions)) {
        readClient.connect();
      }
    } finally {
      if (testServer1 != null) {
        testServer1.shutdown();
      }
      if (testServer2 != null) {
        testServer2.shutdown();
      }
    }
  }
}
