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
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.RetryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class PooledWriteClientFactoryTest {

  @Test
  public void writeAndReadRecords() {
    int[] shuffleIdTestValues = new int[] {1, 7};

    PooledWriteClientFactory writeClientFactory = new PooledWriteClientFactory(TestConstants.CONNECTION_IDLE_TIMEOUT_MILLIS);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    short numSplits = 9;

    try {
      long taskAttemptIdTestValue = 3;
      int appIdSuffix = 1;
      // for each compressBufferSize value, we use a new application which will cause a new connection
      String appId = "app" + (appIdSuffix++);
      for (int shuffleId : shuffleIdTestValues) {
        String appAttempt = "attempt1";
        int numMaps = 1;
        int numPartitions = 10;
        int mapId = 2;
        long taskAttemptId = taskAttemptIdTestValue++;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

        try (RecordSyncWriteClient writeClient = writeClientFactory.getOrCreateClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", appId, appAttempt, new ShuffleWriteConfig("", numSplits))) {
          writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

          writeClient.sendRecord(1, null, null);
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap(new byte[0]));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

          writeClient.sendRecord(2,
              null,
              ByteBuffer.wrap(new byte[0]));

          writeClient.sendRecord(3,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

          writeClient.finishUpload();
        }

        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertNull(record.getKey());
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNull(record);
        }

        appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNull(record);
        }

        appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNull(record);
        }
      }
    } finally {
      testServer1.shutdown();
    }

    Assert.assertEquals(writeClientFactory.getNumCreatedClients(), 1);
    Assert.assertEquals(writeClientFactory.getNumIdleClients(), 1);

    writeClientFactory.shutdown();
  }

  @Test
  public void veryShortIdleTimeout() {
    // use very short idle timeout, so all idle clients should be closed
    long idleTimeout = 10;

    PooledWriteClientFactory writeClientFactory = new PooledWriteClientFactory(idleTimeout);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    int numApps = 3;

    try {
      long taskAttemptIdTestValue = 3;
      int appIdSuffix = 1;
      for (int i = 0; i < numApps; i++) {
        // for each compressBufferSize value, we use a new application which will cause a new connection
        String appId = "app" + (appIdSuffix++);
        int shuffleId = 1;

        String appAttempt = "attempt1";
        int numMaps = 1;
        int numPartitions = 10;
        int mapId = 2;
        long taskAttemptId = taskAttemptIdTestValue++;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

        try (RecordSyncWriteClient writeClient = writeClientFactory.getOrCreateClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", appId, appAttempt, TestConstants.SHUFFLE_WRITE_CONFIG)) {
          writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);

          writeClient.sendRecord(1, null, null);
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap(new byte[0]));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("".getBytes(StandardCharsets.UTF_8)));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
          writeClient.sendRecord(1,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

          writeClient.sendRecord(2,
              null,
              ByteBuffer.wrap(new byte[0]));

          writeClient.sendRecord(3,
              null,
              ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

          writeClient.finishUpload();
        }

        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertNull(record.getKey());
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "");

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNull(record);
        }

        appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(record.getValue(), new byte[0]);

          record = readClient.readRecord();
          Assert.assertNull(record);
        }

        appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
        try (PlainRecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          readClient.connect();
          RecordKeyValuePair record = readClient.readRecord();
          Assert.assertNotNull(record);
          Assert.assertEquals(record.getKey(), null);
          Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

          record = readClient.readRecord();
          Assert.assertNull(record);
        }
      }

      // wait sometime so the background thread could close idle clients
      boolean noIdleClient = RetryUtils.retryUntilTrue(idleTimeout, idleTimeout * 10, ()->{return writeClientFactory.getNumIdleClients() == 0;});
      Assert.assertTrue(noIdleClient);
      Assert.assertEquals(writeClientFactory.getNumCreatedClients(), numApps);
    } finally {
      testServer1.shutdown();
    }

    writeClientFactory.shutdown();
  }
}
