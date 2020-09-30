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
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssFinishUploadException;
import com.uber.rss.exceptions.RssMissingShuffleWriteConfigException;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.exceptions.RssServerBusyException;
import com.uber.rss.exceptions.RssShuffleDataNotAvailableException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.metadata.InMemoryServiceRegistry;
import com.uber.rss.metadata.ServiceRegistry;
import com.uber.rss.testutil.ClientTestUtils;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.RetryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@Test
public class WriteClientEdgeCaseTest {
  @Test
  public void closeClientMultiTimes() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();
    try {
      AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt())) {
        client.close();
        client.close();
      }

      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt())) {
        client.startUpload(appTaskAttemptId1, 1, 20);

        client.close();
        client.close();
      }

      AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app1", "exec1", 1, 2, 2L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId2.getAppId(), appTaskAttemptId2.getAppAttempt())) {
        client.startUpload(appTaskAttemptId2, 1, 20);
        client.sendRecord(1, null, null);

        client.close();
        client.close();
      }

      AppTaskAttemptId appTaskAttemptId3 = new AppTaskAttemptId("app1", "exec1", 1, 2, 3L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId3.getAppId(), appTaskAttemptId3.getAppAttempt())) {
        client.startUpload(appTaskAttemptId3, 1, 20);
        client.sendRecord(1, null, null);
        client.finishUpload();

        client.close();
        client.close();
      }
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test
  public void closeClientAfterServerShutdown() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.sendRecord(1, null, null);
        client.finishUpload();

        List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
        Assert.assertEquals(records.size(), 1);

        // Shutdown server first
        testServer.shutdown();

        // Use another client to test and wait util the server has been shutdown
        boolean serverShutdown = RetryUtils.retryUntilTrue(100, 10000, () -> {
          try (SingleServerWriteClient clientToTestServerConnection = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
            clientToTestServerConnection.startUpload(appTaskAttemptId, 1, 20);
            return false;
          } catch (RssNetworkException ex) {
            return true;
          } catch (Throwable ex) {
            return false;
          }
        });

        Assert.assertTrue(serverShutdown);

        // Close client after server shutdown
        client.close();
        client.close();
      }
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = RssFinishUploadException.class)
  public void duplicateUploadWithsSameTaskAttemptId() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.sendRecord(1, null, null);
        client.finishUpload();
      }

      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.sendRecord(1, null, null);
        client.finishUpload();
      }

      List<RecordKeyValuePair> readRecords = StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(readRecords.size(), 1);
      Assert.assertNull(readRecords.get(0).getKey());
      Assert.assertNull(readRecords.get(0).getValue());
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test
  // This is to test scenario:
  // 1. Task 1 is sending data.
  // 2. Spark driver thinks task 1 lost (e.g. lost its executor), and retries with task 2.
  // 3. Task 2 finishes sending data.
  // 4. Task 1 is still running and trying to finish upload.
  // 5. Read data, should get data written by task 2
  public void staleTaskAttemptThrowsExceptionOnFinishUpload() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      // task 1 sends data but does not finish upload
      AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client1 = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt())) {
        client1.startUpload(appTaskAttemptId1, 1, 20);
        client1.sendRecord(1,
            null,
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

        // task 2 sends data and finish upload
        AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app1", "exec1", 1, 2, 2L);
        try (SingleServerWriteClient client2 = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt())) {
          client2.startUpload(appTaskAttemptId2, 1, 20);
          client2.sendRecord(1,
              null,
              ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
          client2.finishUpload();
        }

        // task 1 finishes upload
        client1.finishUpload();

        List<RecordKeyValuePair> readRecords = StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), appTaskAttemptId1.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId2.getTaskAttemptId()));
        Assert.assertEquals(readRecords.size(), 1);
        Assert.assertEquals(readRecords.get(0).getKey(), null);
        Assert.assertEquals(new String(readRecords.get(0).getValue(), StandardCharsets.UTF_8), "value2");
      }
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test
  public void writeNoRecord() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
        client.finishUpload();
      }

      List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
      Assert.assertEquals(records.size(), 0);
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = {RssMissingShuffleWriteConfigException.class, RssShuffleStageNotStartedException.class, RssShuffleDataNotAvailableException.class})
  public void writeNoRecordWithoutFinishUpload() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 1L);
      try (SingleServerWriteClient client = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
        client.startUpload(appTaskAttemptId, 1, 20);
      }

      int dataAvailableWaitTime = 500;
      StreamServerTestUtils.readAllRecords2(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 0, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataAvailableWaitTime);
    }
    finally {
      testServer.shutdown();
    }
  }

  @Test(expectedExceptions = RssServerBusyException.class)
  public void writeClientsExceedStreamServerMaxConnectionsWithRetry() {
    StreamServerConfig serverConfig = new StreamServerConfig();
    serverConfig.setMaxConnections(1);
    serverConfig.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER);
    serverConfig.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER);

    ServiceRegistry serviceRegistry = new InMemoryServiceRegistry();
    TestStreamServer testServer = TestStreamServer.createRunningServer(serverConfig, serviceRegistry);

    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

    boolean finishUploadAck = true;

    try (SingleServerWriteClient writeclient1 = new CompressedRecordSyncWriteClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", appTaskAttemptId1.getAppId(), appTaskAttemptId1.getAppAttempt(), TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG)) {
      writeclient1.connect();
      writeclient1.startUpload(appTaskAttemptId1, 10, 20);

      writeclient1.sendRecord(1, null, null);

      writeclient1.sendRecord(2,
          ByteBuffer.wrap(new byte[0]),
          ByteBuffer.wrap(new byte[0]));

      writeclient1.sendRecord(3,
          ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
          ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

      writeclient1.finishUpload();

      AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId("app1", "exec1", 1, 3, 0L);

      try (ServerBusyRetriableWriteClient retriableWriteClient =
               new ServerBusyRetriableWriteClient(
                   ()->new CompressedRecordSyncWriteClient("localhost", testServer.getShufflePort(), 500, finishUploadAck, "user1", appTaskAttemptId2.getAppId(), appTaskAttemptId2.getAppAttempt(), TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG),
                   1000,
                   "user1",
                   appTaskAttemptId2.getAppId(),
                   appTaskAttemptId2.getAppAttempt())) {
        retriableWriteClient.connect();
        retriableWriteClient.startUpload(appTaskAttemptId2, 10, 20);
      }
    } finally {
      testServer.shutdown();
    }
  }
}
