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
import com.uber.rss.common.AppMapId;
import com.uber.rss.common.FilePathAndLength;
import com.uber.rss.common.MapTaskCommitStatus;
import com.uber.rss.common.PartitionFilePathAndLength;
import com.uber.rss.exceptions.RssFileCorruptedException;
import com.uber.rss.messages.ShuffleStageStatus;
import com.uber.rss.storage.ShuffleFileUtils;
import com.uber.rss.storage.ShuffleStorage;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/***
 * This class stores state for a shuffle stage when the shuffle executor is writing shuffle data
 */
public class ExecutorShuffleStageState {
    private static final Logger logger =
            LoggerFactory.getLogger(ExecutorShuffleStageState.class);
    
    private final AppShuffleId appShuffleId;
    private final ShuffleWriteConfig appConfig;

    // previously save files before server restarts
    // key is partition, value is files and their lengths
    private final Map<Integer, Map<String, Long>> finalizedFiles = new HashMap<>();

    private int fileStartIndex;

    private int numMaps;
    private int numPartitions;

    // This field stores shuffle output writers. There is one writer per 
    // shuffle partition.
    private final Map<Integer, ShufflePartitionWriter> writers = new HashMap<>();

    private byte fileStatus = ShuffleStageStatus.FILE_STATUS_OK;

    // TODO optimize this
    private final Set<AppTaskAttemptId> pendingFlushMapAttempts = new HashSet<>();

    private final Map<AppMapId, TaskAttemptCollection> taskAttempts = new HashMap<>();

    /***
     * Create an stage state instance
     * @param appShuffleId app shuffle id
     * @param appConfig shuffle write config
     */
    public ExecutorShuffleStageState(AppShuffleId appShuffleId, ShuffleWriteConfig appConfig) {
        this(appShuffleId, appConfig, 0);
    }

    /***
     * Create an stage state instance
     * @param appShuffleId app shuffle id
     * @param appConfig shuffle write config
     * @param fileStartIndex start index for file name suffix
     */
    public ExecutorShuffleStageState(AppShuffleId appShuffleId, ShuffleWriteConfig appConfig, int fileStartIndex) {
        this.appShuffleId = appShuffleId;
        this.appConfig = appConfig;
        this.fileStartIndex = fileStartIndex;
    }

  public synchronized AppShuffleId getAppShuffleId() {
    return appShuffleId;
  }

  public synchronized ShuffleWriteConfig getWriteConfig() {
        return appConfig;
    }

    public synchronized int getFileStartIndex() {
        return fileStartIndex;
    }

    public synchronized void setFileStartIndex(int value) {
        if (value < this.fileStartIndex + appConfig.getNumSplits()) {
            throw new RssInvalidStateException(String.format(
                "New file start index %s cannot be less than current start index %s plus num of splits %s",
                value, this.fileStartIndex, appConfig.getNumSplits()
            ));
        }
        this.fileStartIndex = value;
    }

    public synchronized int getNumMaps() {
        return numMaps;
    }

    public synchronized int getNumPartitions() {
        return numPartitions;
    }

    public synchronized void setNumMapsPartitions(int numMaps, int numPartitions) {
        if (this.numMaps != 0 && this.numMaps != numMaps) {
            throw new RssInvalidStateException(String.format(
                    "Inconsistent value for number of maps, old value: %s, new value %s, app shuffle %s",
                    this.numMaps, numMaps, appShuffleId));
        }
        
        this.numMaps = numMaps;

        if (this.numPartitions != 0 && this.numPartitions != numPartitions) {
            throw new RssInvalidStateException(String.format(
                "Inconsistent value for number of partitions, old value: %s, new value %s, app shuffle %s",
                this.numPartitions, numPartitions, appShuffleId));
        }

        this.numPartitions = numPartitions;
    }

    public synchronized void addFinalizedFiles(Collection<PartitionFilePathAndLength> finalizedFiles) {
        for (PartitionFilePathAndLength entry: finalizedFiles) {
            Map<String, Long> map = this.finalizedFiles.get(entry.getPartition());
            if (map == null) {
                map = new HashMap<>();
                this.finalizedFiles.put(entry.getPartition(), map);
            }
            long length = map.getOrDefault(entry.getPath(), 0L);
            if (entry.getLength() >= length) {
                map.put(entry.getPath(), entry.getLength());
            }
        }
    }

    public synchronized void markMapAttemptStartUpload(AppTaskAttemptId appTaskAttemptId) {
        AppMapId appMapId = appTaskAttemptId.getAppMapId();
        TaskAttemptIdAndState taskState = getTaskState(appMapId, appTaskAttemptId.getTaskAttemptId());
        taskState.markStartUpload();
    }

    public synchronized void markMapAttemptFinishUpload(AppTaskAttemptId appTaskAttemptId) {
        AppMapId appMapId = appTaskAttemptId.getAppMapId();
        TaskAttemptIdAndState taskState = getTaskState(appMapId, appTaskAttemptId.getTaskAttemptId());
        taskState.markFinishUpload();
    }
    
    public synchronized boolean allLatestTaskAttemptsCommitted() {
        // TODO support spark.speculation execution later
        return taskAttempts.values().stream().allMatch(t1->{
            TaskAttemptIdAndState task = t1.getLatestTaskOrNull();
            return task == null || task.isCommitted();
        });
    }

    public synchronized boolean isMapAttemptFinishedUpload(AppTaskAttemptId appTaskAttemptId) {
        return getTaskState(appTaskAttemptId.getAppMapId(), appTaskAttemptId.getTaskAttemptId()).isFinishedUpload();
    }

    public synchronized boolean isMapAttemptCommitted(AppTaskAttemptId appTaskAttemptId) {
        return getTaskState(appTaskAttemptId.getAppMapId(), appTaskAttemptId.getTaskAttemptId()).isCommitted();
    }

    public synchronized ShufflePartitionWriter getOrCreateWriter(int partition, String rootDir, ShuffleStorage storage, boolean fsyncEnabled) {
        if (partition < 0) {
            throw new RssInvalidDataException("Invalid partition: " + partition);
        }

        // TODO use array for writers instead of using map

        ShufflePartitionWriter writer = writers.get(partition);
        if (writer != null) {
            return writer;
        }

        AppShufflePartitionId appShufflePartitionId = new AppShufflePartitionId(
                appShuffleId, partition);

        return writers.computeIfAbsent(partition, p -> {
            String path = ShuffleFileUtils.getShuffleFilePath(
                    rootDir, appShuffleId, partition);
            ShufflePartitionWriter streamer
                    = new ShufflePartitionWriter(appShufflePartitionId,
                    path, fileStartIndex, appConfig.getFileCompressionCodec(), storage, fsyncEnabled, appConfig.getNumSplits());
            return streamer;
        });
    }

    public synchronized void flushAllPartitions() {
        List<ShufflePartitionWriter> writersCopy = new ArrayList<>();
        writersCopy.addAll(writers.values());

        writersCopy.forEach(writer->writer.flush());
    }

    public synchronized void closeWriters() {
        for (ShufflePartitionWriter writer: writers.values()) {
          writer.close();
        }
    }

    public synchronized int getNumOpenedWriters() {
        return (int)writers.values().stream().filter(t->!t.isClosed()).count();
    }

    /**
     * Get persisted bytes for the given partition
     * @return list of files and their length
     */
    public synchronized List<FilePathAndLength> getPersistedBytesSnapshot(int partition) {
        List<FilePathAndLength> result = new ArrayList<>();

        Map<String, Long> map = finalizedFiles.get(partition);
        if (map != null) {
            for (Map.Entry<String, Long> entry: map.entrySet()) {
                result.add(new FilePathAndLength(entry.getKey(), entry.getValue()));
            }
        }

        ShufflePartitionWriter writer = writers.get(partition);
        if (writer == null) {
            return result;
        }

        result.addAll(writer.getPersistedBytesSnapshot());

        // Check whether there is duplicated files
        checkDuplicateFiles(result, partition);

        return result;
    }

    /**
     * Get persisted bytes for all partitions
     * @return list of partition files and their length
     */
    public synchronized List<PartitionFilePathAndLength> getPersistedBytesSnapshots() {
        List<PartitionFilePathAndLength> result = new ArrayList<>();

        for (Map.Entry<Integer, Map<String, Long>> finalizedFileEntry: finalizedFiles.entrySet()) {
            int partition = finalizedFileEntry.getKey();
            Map<String, Long> files = finalizedFileEntry.getValue();
            for (Map.Entry<String, Long> fileEntry: files.entrySet()) {
                result.add(new PartitionFilePathAndLength(partition, fileEntry.getKey(), fileEntry.getValue()));
            }
        }

        for (Map.Entry<Integer, ShufflePartitionWriter> entry: writers.entrySet()) {
            Integer partition = entry.getKey();
            ShufflePartitionWriter writer = entry.getValue();
            List<FilePathAndLength> list = writer.getPersistedBytesSnapshot();
            for (FilePathAndLength filePathAndLength: list) {
                result.add(new PartitionFilePathAndLength(partition, filePathAndLength.getPath(), filePathAndLength.getLength()));
            }
        }

        // Check whether there is duplicated files
        checkDuplicateFiles(result);

        return result;
    }

    /***
     * Get total persisted bytes for all partitions.
     * @return
     */
    public synchronized long getPersistedBytes() {
        long result = 0;
        for (ShufflePartitionWriter writer: writers.values()) {
            result += writer.getPersistedBytes();
        }
        return result;
    }

    /***
     * Get all file locations.
     * @return
     */
    public synchronized List<String> getFileLocations() {
        return writers.values().stream().flatMap(t->t.getFileLocations().stream()).collect(Collectors.toList());
    }

    /***
     * Set last successful task attempt id for a given map id
     * @param mapId
     * @param taskId
     */
    public synchronized void commitMapTask(int mapId, long taskId) {
        TaskAttemptIdAndState taskState = getTaskState(new AppMapId(appShuffleId, mapId), taskId);
        taskState.markCommitted();

        pendingFlushMapAttempts.remove(new AppTaskAttemptId(appShuffleId, mapId, taskId));
    }

    /***
     * Get stage status, which contains map task commit status (last successful map task attempt id)
     * @return
     */
    public synchronized ShuffleStageStatus getShuffleStageStatus() {
        HashMap<Integer, Long> committedMapTaskIds = new HashMap<>(taskAttempts.size());
        for (Map.Entry<AppMapId, TaskAttemptCollection> entry: taskAttempts.entrySet()) {
            int mapId = entry.getKey().getMapId();
            TaskAttemptCollection taskAttemptCollection = entry.getValue();
            // TODO support spark.speculation execution later
            TaskAttemptIdAndState latestTaskAttempt = taskAttemptCollection.getLatestTaskOrNull();
            if (latestTaskAttempt != null && latestTaskAttempt.isCommitted()) {
                committedMapTaskIds.put(mapId, latestTaskAttempt.getTaskAttemptId());
            }
        }

        MapTaskCommitStatus mapTaskCommitStatus = new MapTaskCommitStatus(numMaps, committedMapTaskIds);
        return new ShuffleStageStatus(fileStatus, mapTaskCommitStatus);
    }

    public synchronized void setFileCorrupted() {
        fileStatus = ShuffleStageStatus.FILE_STATUS_CORRUPTED;
    }

    public synchronized byte getFileStatus() {
        return fileStatus;
    }

    // add a task attempt as pending for flush
    public synchronized void addPendingFlushMapAttempt(AppTaskAttemptId appTaskAttemptId) {
        pendingFlushMapAttempts.add(appTaskAttemptId);
    }

    // fetch map task attempts for which we need to flush shuffle files
    public synchronized Collection<AppTaskAttemptId> fetchFlushMapAttempts() {
        if (taskAttempts.size() >= numMaps) {
            return new ArrayList<>(pendingFlushMapAttempts);
        } else {
            return Collections.emptyList();
        }
    }

    public synchronized Collection<AppTaskAttemptId> getPendingFlushMapAttempts() {
      return Collections.unmodifiableCollection(pendingFlushMapAttempts);
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("ExecutorShuffleStageState %s:", appShuffleId));

        sb.append(String.format(", write config: %s", appConfig));
        sb.append(String.format(", file start index: %s", fileStartIndex));
        sb.append(String.format(", maps: %s", numMaps));
        sb.append(String.format(", partitions: %s", numPartitions));

        sb.append(System.lineSeparator());
        sb.append("Writers:");
        for (Map.Entry<Integer, ShufflePartitionWriter> entry: writers.entrySet()) {
            sb.append(System.lineSeparator());
            sb.append(entry.getKey());
            sb.append("->");
            sb.append(entry.getValue());
        }
        return sb.toString();
    }

    private TaskAttemptIdAndState getTaskState(AppMapId appMapId, Long taskAttemptId) {
        TaskAttemptCollection taskCollection = taskAttempts.get(appMapId);
        if (taskCollection == null) {
            taskCollection = new TaskAttemptCollection(appMapId);
            taskAttempts.put(appMapId, taskCollection);
        }
        return taskCollection.getTask(taskAttemptId);
    }

    private void checkDuplicateFiles(List<FilePathAndLength> result, int partition) {
        List<String> filePaths = result.stream().map(t->t.getPath()).collect(Collectors.toList());
        List<String> distinctFilePaths = filePaths.stream().distinct().collect(Collectors.toList());
        if (filePaths.size() != distinctFilePaths.size()) {
            throw new RssFileCorruptedException(String.format(
                "Found duplicate files in partition %s file list: %s",
                partition,
                StringUtils.join(filePaths, ',')));
        }
    }

    private void checkDuplicateFiles(List<PartitionFilePathAndLength> result) {
        List<String> filePaths = result.stream().map(t->t.getPath()).collect(Collectors.toList());
        List<String> distinctFilePaths = filePaths.stream().distinct().collect(Collectors.toList());
        if (filePaths.size() != distinctFilePaths.size()) {
            throw new RssFileCorruptedException(String.format(
                "Found duplicate files in all partition file list: %s",
                StringUtils.join(filePaths, ',')));
        }
    }
}
