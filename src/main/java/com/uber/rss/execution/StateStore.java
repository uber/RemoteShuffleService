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

import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.MapTaskAttemptId;
import com.uber.rss.common.PartitionFilePathAndLength;

import java.util.Collection;

public interface StateStore extends AutoCloseable {

  void storeStageInfo(AppShuffleId appShuffleId, StagePersistentInfo info);

  void storeTaskAttemptCommit(AppShuffleId appShuffleId,
                              Collection<Long> committedTaskAttempts,
                              Collection<PartitionFilePathAndLength> partitionFilePathAndLengths);

  void storeAppDeletion(String appId);

  void storeStageCorruption(AppShuffleId appShuffleId);

  void commit();

  LocalFileStateStoreIterator loadData();

  void close();
}
