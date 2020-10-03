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

package com.uber.rss.execution;

import com.uber.rss.clients.ShuffleWriteConfig;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.MapTaskAttemptId;
import com.uber.rss.common.PartitionFilePathAndLength;
import com.uber.rss.messages.AppDeletionStateItem;
import com.uber.rss.messages.BaseMessage;
import com.uber.rss.messages.CommitMarkerStateItem;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.ShuffleStageStatus;
import com.uber.rss.messages.StageCorruptionStateItem;
import com.uber.rss.messages.StageInfoStateItem;
import com.uber.rss.messages.TaskAttemptCommitStateItem;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class LocalFileLocalFileStateStoreIteratorTest {

  @Test
  public void nonExistingFiles() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(Paths.get(tempPath.toString(), "nonExistFile").toString()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(Paths.get(tempPath.toString(), "nonExistFile1").toString(),
            Paths.get(tempPath.toString(), "nonExistFile2").toString()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void emptyFiles() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    File file1 = Paths.get(tempPath.toString(), "file1").toFile();
    file1.deleteOnExit();
    try (FileOutputStream fileOutputStream1 = new FileOutputStream(file1)) {
    }

    File file2 = Paths.get(tempPath.toString(), "file2").toFile();
    file2.deleteOnExit();
    try (FileOutputStream fileOutputStream2 = new FileOutputStream(file2)) {
    }

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void corruptedFile() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    File file1 = Paths.get(tempPath.toString(), "file1").toFile();
    file1.deleteOnExit();
    try (FileOutputStream fileOutputStream1 = new FileOutputStream(file1)) {
      fileOutputStream1.write(1);
    }

    File file2 = Paths.get(tempPath.toString(), "file2").toFile();
    file2.deleteOnExit();
    try (FileOutputStream fileOutputStream2 = new FileOutputStream(file2)) {
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(-1));
      fileOutputStream2.write(1);
    }

    File file3 = Paths.get(tempPath.toString(), "file3").toFile();
    file3.deleteOnExit();
    try (FileOutputStream fileOutputStream3 = new FileOutputStream(file3)) {
      fileOutputStream3.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream3.write(ByteBufUtils.convertIntToBytes(2));
      fileOutputStream3.write(1);
    }

    File file4 = Paths.get(tempPath.toString(), "file4").toFile();
    file4.deleteOnExit();
    try (FileOutputStream fileOutputStream4 = new FileOutputStream(file4)) {
      fileOutputStream4.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream4.write(ByteBufUtils.convertIntToBytes(2));
      fileOutputStream4.write(1);
      fileOutputStream4.write(1);
    }

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath(), file3.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());

    iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath(), file3.getAbsolutePath(), file4.getAbsolutePath()));
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void commitMarker() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    AppShuffleId appShuffleId1 = new AppShuffleId("app1", "1", 2);
    ShuffleWriteConfig shuffleWriteConfig1 = new ShuffleWriteConfig("gzip", (short)6);
    StageInfoStateItem stageInfoStateItem1 = new StageInfoStateItem(appShuffleId1,
        4, 5, shuffleWriteConfig1, ShuffleStageStatus.FILE_STATUS_OK);
    ByteBuf stageInfoStateItemBuf1 = Unpooled.buffer();
    stageInfoStateItem1.serialize(stageInfoStateItemBuf1);
    byte[] stageInfoStateItemBytes1;
    stageInfoStateItemBytes1 = ByteBufUtils.readBytes(stageInfoStateItemBuf1);
    stageInfoStateItemBuf1.release();

    ShuffleWriteConfig shuffleWriteConfig2 = new ShuffleWriteConfig("gzip", (short)60);
    AppShuffleId appShuffleId2 = new AppShuffleId("app10", "10", 20);
    StageInfoStateItem stageInfoStateItem2 = new StageInfoStateItem(appShuffleId2,
        40, 50, shuffleWriteConfig2, ShuffleStageStatus.FILE_STATUS_OK);
    ByteBuf stageInfoStateItemBuf2 = Unpooled.buffer();
    stageInfoStateItem2.serialize(stageInfoStateItemBuf2);
    byte[] stageInfoStateItemBytes2;
    stageInfoStateItemBytes2 = ByteBufUtils.readBytes(stageInfoStateItemBuf2);
    stageInfoStateItemBuf2.release();

    CommitMarkerStateItem commitMarkerStateItem1 = new CommitMarkerStateItem(System.currentTimeMillis());
    ByteBuf commitMarkerStateItemBuf1 = Unpooled.buffer();
    commitMarkerStateItem1.serialize(commitMarkerStateItemBuf1);
    byte[] commitMarkerStateItemBytes1;
    commitMarkerStateItemBytes1 = ByteBufUtils.readBytes(commitMarkerStateItemBuf1);
    commitMarkerStateItemBuf1.release();

    File file1 = Paths.get(tempPath.toString(), "file1").toFile();
    file1.deleteOnExit();
    try (FileOutputStream fileOutputStream1 = new FileOutputStream(file1)) {
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes1.length));
      fileOutputStream1.write(stageInfoStateItemBytes1);

      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes2.length));
      fileOutputStream1.write(stageInfoStateItemBytes2);

      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_CommitMarkerStateItem));
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(commitMarkerStateItemBytes1.length));
      fileOutputStream1.write(commitMarkerStateItemBytes1);

      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes1.length));
      fileOutputStream1.write(stageInfoStateItemBytes1);

      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream1.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes2.length));
      fileOutputStream1.write(stageInfoStateItemBytes2);
    }

    int numRepeatTestItems = 5;

    File file2 = Paths.get(tempPath.toString(), "file2").toFile();
    file2.deleteOnExit();
    try (FileOutputStream fileOutputStream2 = new FileOutputStream(file2)) {
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes2.length));
      fileOutputStream2.write(stageInfoStateItemBytes2);

      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_CommitMarkerStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(commitMarkerStateItemBytes1.length));
      fileOutputStream2.write(commitMarkerStateItemBytes1);

      for (int i = 0; i < numRepeatTestItems; i++) {
        fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_StageInfoStateItem));
        fileOutputStream2.write(ByteBufUtils.convertIntToBytes(stageInfoStateItemBytes1.length));
        fileOutputStream2.write(stageInfoStateItemBytes1);
      }

      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_CommitMarkerStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(commitMarkerStateItemBytes1.length));
      fileOutputStream2.write(commitMarkerStateItemBytes1);

      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(MessageConstants.MESSAGE_CommitMarkerStateItem));
      fileOutputStream2.write(ByteBufUtils.convertIntToBytes(commitMarkerStateItemBytes1.length));
      fileOutputStream2.write(commitMarkerStateItemBytes1);
    }

    LocalFileStateStoreIterator iterator = new LocalFileStateStoreIterator(
        Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath()));
    for (int i = 0; i < 100; i++) {
      Assert.assertTrue(iterator.hasNext());
    }

    Assert.assertTrue(iterator.hasNext());
    BaseMessage item = iterator.next();
    StageInfoStateItem stageInfoStateItem = (StageInfoStateItem)item;
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 4);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 5);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);

    Assert.assertTrue(iterator.hasNext());
    item = iterator.next();
    stageInfoStateItem = (StageInfoStateItem)item;
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId2);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 40);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 50);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig2);

    Assert.assertTrue(iterator.hasNext());
    item = iterator.next();
    stageInfoStateItem = (StageInfoStateItem)item;
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId2);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 40);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 50);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig2);

    for (int i = 0; i < numRepeatTestItems; i++) {
      Assert.assertTrue(iterator.hasNext());
      item = iterator.next();
      stageInfoStateItem = (StageInfoStateItem) item;
      Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
      Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 4);
      Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 5);
      Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);
    }

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());
  }

  @Test
  public void writeAndReadData() throws IOException {
    Path tempPath = Files.createTempDirectory("StateStoreTest");
    tempPath.toFile().deleteOnExit();

    AppShuffleId appShuffleId1 = new AppShuffleId("app1", "1", 2);
    AppTaskAttemptId appTaskAttemptId1 = new AppTaskAttemptId(appShuffleId1, 1, 99L);
    ShuffleWriteConfig shuffleWriteConfig1 = new ShuffleWriteConfig("gzip", (short) 6);
    PartitionFilePathAndLength partitionFilePathAndLength1 = new PartitionFilePathAndLength(1, "file1", 123);

    LocalFileStateStore store = new LocalFileStateStore(tempPath.toString());
    store.storeStageInfo(appShuffleId1, new StagePersistentInfo(4, 5, shuffleWriteConfig1, ShuffleStageStatus.FILE_STATUS_OK));
    store.storeTaskAttemptCommit(appShuffleId1,
        Arrays.asList(new MapTaskAttemptId(appTaskAttemptId1.getMapId(), appTaskAttemptId1.getTaskAttemptId())),
        Arrays.asList(partitionFilePathAndLength1));
    store.storeAppDeletion("deletedApp");
    store.storeAppDeletion(appShuffleId1.getAppId());
    store.storeStageCorruption(appShuffleId1);
    store.commit();
    store.close();

    store = new LocalFileStateStore(tempPath.toString());
    LocalFileStateStoreIterator iterator = store.loadData();

    Assert.assertTrue(iterator.hasNext());
    BaseMessage dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof StageInfoStateItem);
    StageInfoStateItem stageInfoStateItem = (StageInfoStateItem)dataItem;
    Assert.assertEquals(stageInfoStateItem.getAppShuffleId(), appShuffleId1);
    Assert.assertEquals(stageInfoStateItem.getNumPartitions(), 4);
    Assert.assertEquals(stageInfoStateItem.getFileStartIndex(), 5);
    Assert.assertEquals(stageInfoStateItem.getWriteConfig(), shuffleWriteConfig1);
    Assert.assertEquals(stageInfoStateItem.getFileStatus(), ShuffleStageStatus.FILE_STATUS_OK);

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof TaskAttemptCommitStateItem);
    TaskAttemptCommitStateItem taskAttemptCommitStateItem = (TaskAttemptCommitStateItem)dataItem;
    Assert.assertEquals(taskAttemptCommitStateItem.getAppShuffleId(), appTaskAttemptId1.getAppShuffleId());
    Assert.assertEquals(taskAttemptCommitStateItem.getMapTaskAttemptIds(), Arrays.asList(new MapTaskAttemptId(appTaskAttemptId1.getMapId(), appTaskAttemptId1.getTaskAttemptId())));
    Assert.assertEquals(taskAttemptCommitStateItem.getPartitionFilePathAndLengths(), Arrays.asList(partitionFilePathAndLength1));

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof AppDeletionStateItem);
    AppDeletionStateItem appDeletionStateItem = (AppDeletionStateItem)dataItem;
    Assert.assertEquals(appDeletionStateItem.getAppId(), "deletedApp");

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof AppDeletionStateItem);
    appDeletionStateItem = (AppDeletionStateItem)dataItem;
    Assert.assertEquals(appDeletionStateItem.getAppId(), appShuffleId1.getAppId());

    Assert.assertTrue(iterator.hasNext());
    dataItem = iterator.next();
    Assert.assertTrue(dataItem instanceof StageCorruptionStateItem);
    StageCorruptionStateItem stageCorruptionStateItem = (StageCorruptionStateItem)dataItem;
    Assert.assertEquals(stageCorruptionStateItem.getAppShuffleId(), appShuffleId1);

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());

    Assert.assertFalse(iterator.hasNext());
    Assert.assertNull(iterator.next());

    iterator.close();
  }
}
