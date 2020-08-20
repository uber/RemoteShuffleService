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

import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.testutil.ClientTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompressedRecordClientTest {

  @Test(expectedExceptions = RssNetworkException.class)
  public void invalidHostName() {
    String appId = "app1";
    String appAttempt = "attempt1";
    int shuffleId = 1;
    int numMaps = 1;
    int numPartitions = 10;
    int mapId = 2;
    long taskAttemptId = 3;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);
    int timeoutMillis = 100;
    try (RecordSyncWriteClient writeClient = new CompressedRecordSyncWriteClient("invalid_host_name", 9999, timeoutMillis, "user1", appId, appAttempt, 0, new ShuffleWriteConfig())) {
      writeClient.connect();
      writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
    }
  }

  @Test
  public void writeAndReadRecords() {
    int[] compressBufferSizeTestValues = new int[] {1, 10, 100, 1000, 1000000};
    short[] numSplitsTestValues = new short[] {1, 2};

    for (int compressBufferSize: compressBufferSizeTestValues) {
      for (short numSplits : numSplitsTestValues) {
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

          try (RecordSyncWriteClient writeClient = new CompressedRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", appAttempt, compressBufferSize, new ShuffleWriteConfig("", numSplits))) {
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
          try (RecordSocketReadClient readClient = new CompressedRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
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

          appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 2);
          try (RecordSocketReadClient readClient = new CompressedRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
            readClient.connect();
            RecordKeyValuePair record = readClient.readRecord();
            Assert.assertNotNull(record);
            Assert.assertEquals(record.getKey(), new byte[0]);
            Assert.assertEquals(record.getValue(), new byte[0]);

            record = readClient.readRecord();
            Assert.assertNull(record);
          }

          appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
          try (RecordSocketReadClient readClient = new CompressedRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
            readClient.connect();
            RecordKeyValuePair record = readClient.readRecord();
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
    }
  }

  @Test
  public void writeAndReadManyRecords() {
    int[] compressBufferSizeTestValues = new int[] {3, 99, 9999, 99999};
    String[] fileCompressionCodecTestValues = new String[] {null, "", "lz4"};
    int fileCompressionCodecTestValueIndex = 0;
    short[] numSplitsTestValues = new short[] {1, 2, 3, 4};
    int numSplitsTestValueIndex = 0;

    for (int compressBufferSize: compressBufferSizeTestValues) {
      String compressionCodec = fileCompressionCodecTestValues[fileCompressionCodecTestValueIndex++ % fileCompressionCodecTestValues.length];
      short numSplits = numSplitsTestValues[numSplitsTestValueIndex++ % numSplitsTestValues.length];
      ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig(compressionCodec, numSplits);

      TestStreamServer testServer1 = TestStreamServer.createRunningServer();

      Map<Integer, List<Pair<String, String>>> mapTaskData1 = new HashMap<>();
      Map<Integer, List<Pair<String, String>>> mapTaskData2 = new HashMap<>();

      mapTaskData1.put(1, ClientTestUtils.writeRecords1);
      mapTaskData1.put(2, ClientTestUtils.writeRecords2);
      mapTaskData1.put(3, ClientTestUtils.writeRecords3);

      mapTaskData2.put(1, ClientTestUtils.writeRecords3);
      mapTaskData2.put(2, ClientTestUtils.writeRecords2);
      mapTaskData2.put(3, ClientTestUtils.writeRecords1);

      try {
        String appId = "app1";
        String appAttempt = "attempt1";
        int shuffleId = 1;
        int numMaps = 2;
        int numPartitions = 10;

        // map task 1 writes data
        int mapId1 = 2;
        long taskAttemptId1 = 30;
        AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId1, taskAttemptId1);
        try (RecordSyncWriteClient writeClient = new CompressedRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", appAttempt, compressBufferSize, shuffleWriteConfig)) {
          ClientTestUtils.connectAndWriteData(mapTaskData1, numMaps, numPartitions, appTaskAttemptId1, writeClient);
        }

        // map task 2 writes data
        int mapId2 = 3;
        long taskAttemptId2 = 40;
        AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId2);
        try (RecordSyncWriteClient writeClient = new CompressedRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", appAttempt, compressBufferSize, shuffleWriteConfig)) {
          ClientTestUtils.connectAndWriteData(mapTaskData2, numMaps, numPartitions, appTaskAttemptId2, writeClient);
        }

        // read data for each partition
        for (Integer partition: mapTaskData1.keySet()) {
          AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, partition);
          try (RecordSocketReadClient readClient = new CompressedRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId1.getTaskAttemptId(), appTaskAttemptId2.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
            List<RecordKeyValuePair> readRecords = ClientTestUtils.readData(appShufflePartitionId, readClient);
            Assert.assertTrue(readRecords.size() > 0);

            readRecords.sort(new Comparator<RecordKeyValuePair>() {
              @Override
              public int compare(RecordKeyValuePair t1, RecordKeyValuePair t2) {
                return UnsignedBytes.lexicographicalComparator().compare(t1.getKey(), t2.getKey());
              }
            });

            List<Pair<String, String>> expectedKeyValuePairs = new ArrayList<>(mapTaskData1.get(partition));
            expectedKeyValuePairs.addAll(mapTaskData2.get(partition));
            expectedKeyValuePairs.sort(Ordering.natural());

            Assert.assertEquals(readRecords.size(), expectedKeyValuePairs.size());

            for (int i = 0; i < readRecords.size(); i++) {
              Assert.assertEquals(new String(readRecords.get(i).getKey(), StandardCharsets.UTF_8), expectedKeyValuePairs.get(i).getKey());
              Assert.assertEquals(new String(readRecords.get(i).getValue(), StandardCharsets.UTF_8), expectedKeyValuePairs.get(i).getValue());
            }
          }
        }
      } finally {
        testServer1.shutdown();
      }
    }
  }
}
