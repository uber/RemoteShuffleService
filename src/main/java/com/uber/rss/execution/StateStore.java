package com.uber.rss.execution;

import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.MapTaskAttemptId;
import com.uber.rss.common.PartitionFilePathAndLength;

import java.util.Collection;

public interface StateStore extends AutoCloseable {

  void storeStageInfo(AppShuffleId appShuffleId, StagePersistentInfo info);

  void storeTaskAttemptCommit(AppShuffleId appShuffleId,
                              Collection<MapTaskAttemptId> committedTaskAttempts,
                              Collection<PartitionFilePathAndLength> partitionFilePathAndLengths);

  void storeAppDeletion(String appId);

  void storeStageCorruption(AppShuffleId appShuffleId);

  void commit();

  LocalFileStateStoreIterator loadData();

  void close();
}
