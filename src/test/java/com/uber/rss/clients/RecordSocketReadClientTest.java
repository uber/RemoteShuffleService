package com.uber.rss.clients;

import com.uber.rss.StreamServerConfig;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.testutil.ClientTestUtils;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RecordSocketReadClientTest {

  @Test
  public void writeAndReadRecords() {
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

      try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, new ShuffleWriteConfig())) {
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
      try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
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
      try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(record.getKey(), new byte[0]);
        Assert.assertEquals(record.getValue(), new byte[0]);

        record = readClient.readRecord();
        Assert.assertNull(record);
      }

      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
      try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
        Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

        record = readClient.readRecord();
        Assert.assertNull(record);
      }

      // read non-existing partition
      appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 999);
      try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
        readClient.connect();
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNull(record);
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void writeAndReadManyRecords() {
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
      try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, new ShuffleWriteConfig())) {
        ClientTestUtils.connectAndWriteData(mapTaskData1, numMaps, numPartitions, appTaskAttemptId1, writeClient);
      }

      testServer1.getShuffleExecutor().pollAndWaitMapAttemptFinishedUpload(appTaskAttemptId1, TestConstants.DATA_AVAILABLE_TIMEOUT);

      // map task 2 writes data
      int mapId2 = 3;
      long taskAttemptId2 = 40;
      AppTaskAttemptId appTaskAttemptId2 = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId2, taskAttemptId2);
      try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, new ShuffleWriteConfig())) {
        ClientTestUtils.connectAndWriteData(mapTaskData2, numMaps, numPartitions, appTaskAttemptId2, writeClient);
      }

      // read data for each partition
      for (Integer partition: mapTaskData1.keySet()) {
        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, partition);
        try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId1.getTaskAttemptId(), appTaskAttemptId2.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
          List<RecordKeyValuePair> readRecords = ClientTestUtils.readData(appShufflePartitionId, readClient);
          Assert.assertTrue(readRecords.size() > 0);
          Assert.assertEquals(readRecords.size(), mapTaskData1.get(partition).size() + mapTaskData2.get(partition).size());
          for (int i = 0; i < mapTaskData1.get(partition).size(); i++) {
            Assert.assertEquals(new String(readRecords.get(i).getKey(), StandardCharsets.UTF_8), mapTaskData1.get(partition).get(i).getKey());
            Assert.assertEquals(new String(readRecords.get(i).getValue(), StandardCharsets.UTF_8), mapTaskData1.get(partition).get(i).getValue());
          }
          for (int i = 0; i < mapTaskData2.get(partition).size(); i++) {
            Assert.assertEquals(new String(readRecords.get(mapTaskData1.get(partition).size() + i).getKey(), StandardCharsets.UTF_8), mapTaskData2.get(partition).get(i).getKey());
            Assert.assertEquals(new String(readRecords.get(mapTaskData1.get(partition).size() + i).getValue(), StandardCharsets.UTF_8), mapTaskData2.get(partition).get(i).getValue());
          }
        }
      }
    } finally {
      testServer1.shutdown();
    }
  }

  @Test
  public void serverRestartAfterWriteFinishUpload() throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(config -> config.setRootDirectory(rootDir));

    String appId = "app1";
    String appAttempt = "attempt1";
    int shuffleId = 1;
    int numMaps = 1;
    int numPartitions = 10;
    int mapId = 2;
    long taskAttemptId = 3;
    AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, new ShuffleWriteConfig())) {
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
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNotNull(record);
    }

    // shutdown server and restart
    testServer1.shutdown();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer(config -> config.setRootDirectory(rootDir));

    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
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
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNotNull(record);
      Assert.assertEquals(record.getKey(), new byte[0]);
      Assert.assertEquals(record.getValue(), new byte[0]);

      record = readClient.readRecord();
      Assert.assertNull(record);
    }

    appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 3);
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNotNull(record);
      Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
      Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

      record = readClient.readRecord();
      Assert.assertNull(record);
    }

    // read non-existing partition
    appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 999);
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNull(record);
    }

    testServer2.shutdown();
  }

  @Test
  public void serverRestartAndWriteClientWriteWithNewTaskAttemptId() throws IOException {
    String rootDir = Files.createTempDirectory("StreamServer_").toString();

    short numSplits = 3;
    ShuffleWriteConfig shuffleWriteConfig = new ShuffleWriteConfig("", numSplits);

    Consumer<StreamServerConfig> configModifier = config -> {
      config.setRootDirectory(rootDir);
      config.setNumSplits(numSplits);
    };

    TestStreamServer testServer1 = TestStreamServer.createRunningServer(configModifier);

    String appId = "app1";
    String appAttempt = "attempt1";
    int shuffleId = 1;
    int numMaps = 2;
    int numPartitions = 10;
    AppTaskAttemptId map1TaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, 1, 0);
    AppTaskAttemptId map2TaskAttemptId1 = new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 3);
    AppTaskAttemptId map2TaskAttemptId2 = new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 30);
    AppTaskAttemptId map2TaskAttemptId3 = new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 31);
    AppTaskAttemptId map2TaskAttemptId4 = new AppTaskAttemptId(appId, appAttempt, shuffleId, 2, 40);

    int numRecords = 1000;

    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map1TaskAttemptId, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap("map1Key".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("map1Value".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId1, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(appId, appAttempt, shuffleId, 1);
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost",
        testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId1.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNotNull(record);
    }

    // now task attempt 1 finishes, shutdown server and restart
    testServer1.shutdown();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer(configModifier);

    // write client writes with new task attempt id
    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId2, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key2".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    // read data
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost",
        testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId2.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      List<Pair<String, String>> allRecords = new ArrayList<>();
      for (int i = 0; i < numRecords * 2; i++) {
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        allRecords.add(Pair.of(new String(record.getKey(), StandardCharsets.UTF_8), new String(record.getValue(), StandardCharsets.UTF_8)));
      }
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNull(record);

      List<Pair<String, String>> map1Records = allRecords.stream().filter(t->t.getKey().equals("map1Key")).collect(Collectors.toList());
      Assert.assertEquals(map1Records.size(), numRecords);
      map1Records.forEach(t -> {
        Assert.assertEquals(t.getKey(), "map1Key");
        Assert.assertEquals(t.getValue(), "map1Value");
      });

      List<Pair<String, String>> otherRecords = allRecords.stream().filter(t->!t.getKey().equals("map1Key")).collect(Collectors.toList());
      Assert.assertEquals(otherRecords.size(), numRecords);
      otherRecords.forEach(t -> {
        Assert.assertEquals(t.getKey(), "key2");
        Assert.assertEquals(t.getValue(), "value2");
      });
    }

    // write client writes with new task attempt id, but does not finish upload
    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer2.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId3, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key3".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)));
      }
    }

    // shutdown server and restart
    testServer2.shutdown();
    TestStreamServer testServer3 = TestStreamServer.createRunningServer(configModifier);

    // write client writes with new task attempt id, and finishes upload
    try (RecordSyncWriteClient writeClient = new PlainRecordSyncWriteClient("localhost", testServer3.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", "app1", appAttempt, shuffleWriteConfig)) {
      writeClient.connect();
      writeClient.startUpload(map2TaskAttemptId4, numMaps, numPartitions);
      for (int i = 0; i < numRecords; i++) {
        writeClient.sendRecord(1,
            ByteBuffer.wrap("key4".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("value4".getBytes(StandardCharsets.UTF_8)));
      }
      writeClient.finishUpload();
    }

    // read data
    try (RecordSocketReadClient readClient = new PlainRecordSocketReadClient("localhost",
        testServer3.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", appShufflePartitionId,
        Arrays.asList(map1TaskAttemptId.getTaskAttemptId(), map2TaskAttemptId4.getTaskAttemptId()),
        TestConstants.DATA_AVAILABLE_POLL_INTERVAL, TestConstants.DATA_AVAILABLE_TIMEOUT)) {
      readClient.connect();
      List<Pair<String, String>> allRecords = new ArrayList<>();
      for (int i = 0; i < numRecords * 2; i++) {
        RecordKeyValuePair record = readClient.readRecord();
        Assert.assertNotNull(record);
        allRecords.add(Pair.of(new String(record.getKey(), StandardCharsets.UTF_8), new String(record.getValue(), StandardCharsets.UTF_8)));
      }
      RecordKeyValuePair record = readClient.readRecord();
      Assert.assertNull(record);

      List<Pair<String, String>> map1Records = allRecords.stream().filter(t->t.getKey().equals("map1Key")).collect(Collectors.toList());
      Assert.assertEquals(map1Records.size(), numRecords);
      map1Records.forEach(t -> {
        Assert.assertEquals(t.getKey(), "map1Key");
        Assert.assertEquals(t.getValue(), "map1Value");
      });

      List<Pair<String, String>> otherRecords = allRecords.stream().filter(t->!t.getKey().equals("map1Key")).collect(Collectors.toList());
      Assert.assertEquals(otherRecords.size(), numRecords);
      otherRecords.forEach(t -> {
        Assert.assertEquals(t.getKey(), "key4");
        Assert.assertEquals(t.getValue(), "value4");
      });
    }

    testServer3.shutdown();
  }
}
