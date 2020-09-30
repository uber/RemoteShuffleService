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

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssInconsistentReplicaException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssNoActiveReadClientException;
import com.uber.rss.exceptions.RssNonRecoverableException;
import com.uber.rss.metrics.M3Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class read shuffle data from multiple replicated shuffle servers (replication group) to achieve fault tolerance.
 * It will try another shuffle server if the first server hits error.
 */
public class ReplicatedReadClient implements MultiServerReadClient {
  private static final Logger logger = LoggerFactory.getLogger(ReplicatedReadClient.class);

  private final ServerReplicationGroup serverReplicationGroup;
  private final int timeoutMillis;
  private final ClientRetryOptions clientRetryOptions;
  private final int readQueueSize;
  private final String user;
  private final AppShufflePartitionId appShufflePartitionId;
  private final Collection<Long> latestTaskAttemptIds;
  private final long dataAvailablePollInterval;
  private final long dataAvailableWaitTime;
  private final RetriableSocketReadClient[] clients;
  private final boolean[] clientsInitialized;

  // Store how many records have been consumed (returned to caller of this class) for each task attempt.
  // It is used to skip records to avoid reading duplicate data when we switch from failed server to another server.
  private final Map<Long, Long> numConsumedRecordsMap = new HashMap<>();
  private final Map<Long, RecordKeyValuePair> lastConsumedRecordsMap = new HashMap<>();

  // Store how many records have been read from current client/server connection for each task attempt.
  // It is used to skip records to avoid reading duplicate data when we switch from failed server to another server.
  private final Map<Long, Long> numReadRecordsMap = new HashMap<>();
  private final Map<Long, RecordKeyValuePair> lastReadRecordsMap = new HashMap<>();

  private final boolean checkDataConsistency;

  private int currentClientIndex = 0;
  private boolean endOfRead = false;

  private long shuffleReadBytes = -1;

  public ReplicatedReadClient(ServerReplicationGroup serverReplicationGroup,
                              int timeoutMillis,
                              int readQueueSize,
                              String user,
                              AppShufflePartitionId appShufflePartitionId,
                              ReadClientDataOptions dataOptions) {
    this(serverReplicationGroup,
        timeoutMillis,
        new ClientRetryOptions(dataOptions.getDataAvailablePollInterval(), timeoutMillis, serverDetail->serverDetail),
        readQueueSize,
        user,
        appShufflePartitionId,
        dataOptions,
        true);
  }

  public ReplicatedReadClient(ServerReplicationGroup serverReplicationGroup,
                              int timeoutMillis,
                              int readQueueSize,
                              String user,
                              AppShufflePartitionId appShufflePartitionId,
                              ReadClientDataOptions dataOptions,
                              boolean checkDataConsistency) {
    this(serverReplicationGroup,
        timeoutMillis,
        new ClientRetryOptions(dataOptions.getDataAvailablePollInterval(), timeoutMillis, serverDetail->serverDetail),
        readQueueSize,
        user,
        appShufflePartitionId,
        dataOptions,
        checkDataConsistency);
  }

  public ReplicatedReadClient(ServerReplicationGroup serverReplicationGroup,
                              int timeoutMillis,
                              ClientRetryOptions retryOptions,
                              int readQueueSize,
                              String user,
                              AppShufflePartitionId appShufflePartitionId,
                              ReadClientDataOptions dataOptions,
                              boolean checkDataConsistency) {
    this.serverReplicationGroup = serverReplicationGroup;
    this.timeoutMillis = timeoutMillis;
    this.clientRetryOptions = retryOptions;
    this.readQueueSize = readQueueSize;
    this.user = user;
    this.appShufflePartitionId = appShufflePartitionId;
    this.latestTaskAttemptIds = dataOptions.getLatestTaskAttemptIds();
    this.dataAvailablePollInterval = dataOptions.getDataAvailablePollInterval();
    this.dataAvailableWaitTime = dataOptions.getDataAvailableWaitTime();
    this.checkDataConsistency = checkDataConsistency;

    List<ServerDetail> servers = serverReplicationGroup.getServers();
    if (servers.isEmpty()) {
      throw new RssException("No server in replication group");
    }

    clients = new RetriableSocketReadClient[servers.size()];
    clientsInitialized = new boolean[clients.length];

    resetClientInstances();
  }

  @Override
  public synchronized void connect() {
    long currentTime = System.currentTimeMillis();
    long startTime = currentTime;

    long maxRetryTimeoutMillis = timeoutMillis * serverReplicationGroup.getServers().size();
    long sleepMillis = dataAvailablePollInterval;

    while (currentTime - startTime <= maxRetryTimeoutMillis) {
      try {
        connectAndInitializeClient();
        break;
      } catch (Throwable ex) {
        currentTime = System.currentTimeMillis();
        if (currentTime + sleepMillis - startTime > maxRetryTimeoutMillis) {
          throw ex;
        } else {
          logger.warn(String.format(
              "Failed to initialize, will wait %s millis and retry to connect to server replication group: %s",
              sleepMillis, serverReplicationGroup),
              ex);
          resetClientInstances();
          try {
            Thread.sleep(sleepMillis);
            sleepMillis*=2;
          } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting", e);
          }
        }
      }
    }
  }

  @Override
  public synchronized void close() {
    if (currentClientIndex < clients.length) {
      if (clientsInitialized[currentClientIndex]) {
        // remember shuffle read bytes because we may not get it back after closing the client
        shuffleReadBytes = clients[currentClientIndex].getShuffleReadBytes();
      }
      clients[currentClientIndex].close();
      clientsInitialized[currentClientIndex] = false;
    }

    numConsumedRecordsMap.clear();
    numReadRecordsMap.clear();

    lastConsumedRecordsMap.clear();
    lastReadRecordsMap.clear();
  }

  @Override
  public synchronized RecordKeyValuePair readRecord() {
    if (endOfRead) {
      return null;
    }

    // check whether there is current active client, ignore return value
    getActiveClient();

    while (currentClientIndex < clients.length) {
      if (endOfRead) {
        return null;
      }

      // There is some internal state like consumed/read record count, we need to make sure updating the state
      // consistently, and throw out error if not. "retriable" flag is to make sure we only retry when the state
      // is consistent.
      boolean retriable = false;

      try {
        if (!clientsInitialized[currentClientIndex]) {
          // client not initialized, need to initialize it
          connectAndInitializeClient();
        }

        retriable = true;
        RecordKeyValuePair record = clients[currentClientIndex].readRecord();
        retriable = false;

        if (clients.length == 1) {
          return record;
        }

        while (record != null) {
          if (shouldSkipReadRecord(record)) {
            rememberLastReadRecord(record);
            retriable = true;
            record = clients[currentClientIndex].readRecord();
            retriable = false;
            continue;
          }

          rememberLastConsumedRecord(record);
          rememberLastReadRecord(record);
          return record;
        }

        // got null record, meaning end of stream
        checkRecordDataConsistency();
        endOfRead = true;
        return null;
      } catch (RssInconsistentReplicaException | RssNonRecoverableException ex) {
        M3Stats.addException(ex, this.getClass().getSimpleName());
        closeClient(currentClientIndex);
        throw ex;
      } catch (Throwable ex) {
        M3Stats.addException(ex, this.getClass().getSimpleName());
        closeClient(currentClientIndex);
        boolean tryMoreClients = currentClientIndex < clients.length - 1;
        if (retriable && tryMoreClients) {
          logger.warn(String.format(
              "Failed to read after reading %s records in client (current index: %s): %s. Will try next client in the replication group",
              numReadRecordsMap.values().stream().mapToLong(t->t).sum(), currentClientIndex, clients[currentClientIndex]),
              ex);
          currentClientIndex++;
        } else if (!tryMoreClients) {
          // last client failed, throw out exception
          throw ex;
        } else {
          throw new RssNonRecoverableException("Failed to read records from server replication group: " + serverReplicationGroup, ex);
        }
      }
    }

    throw new RssInvalidStateException("Should not execute here!");
  }

  @Override
  public synchronized long getShuffleReadBytes() {
    if (shuffleReadBytes >= 0) {
      return shuffleReadBytes;
    }
    return getActiveClient().getShuffleReadBytes();
  }

  @Override
  public String toString() {
    return "ReplicatedReadClient{" +
        "clients=" + Arrays.toString(clients) +
        '}';
  }

  private void resetClientInstances() {
    List<ServerDetail> servers = serverReplicationGroup.getServers();
    for (int i = 0; i < servers.size(); i++) {
      ServerDetail serverDetail = servers.get(i);
      RetriableSocketReadClient client = new RetriableSocketReadClient(serverDetail,
          timeoutMillis,
          clientRetryOptions,
          readQueueSize,
          user,
          appShufflePartitionId,
          new ReadClientDataOptions(latestTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime));
      clients[i] = client;
      clientsInitialized[i] = false;
    }
  }

  private void connectAndInitializeClient() {
    List<ExceptionLogInfo> exceptions = null;
    boolean succeeded = false;
    for (; currentClientIndex < clients.length; currentClientIndex++) {
      try {
        logger.info(String.format("Trying to connect to server: %s", clients[currentClientIndex]));
        clients[currentClientIndex].connect();
        clientsInitialized[currentClientIndex] = true;
        resetReadRecords();
        succeeded = true;
        break;
      } catch (Throwable ex) {
        M3Stats.addException(ex, this.getClass().getSimpleName());
        closeClient(currentClientIndex);
        if (exceptions == null) {
          exceptions = new ArrayList<>();
        }
        exceptions.add(new ExceptionLogInfo("Failed to initialize: " + clients[currentClientIndex].toString(), ex));

        if (currentClientIndex >= clients.length - 1) {
          // last client failed, throw out exception
          throw new RssAggregateException(exceptions.stream().map(t -> t.exception).collect(Collectors.toList()));
        }
      }
    }

    if (!succeeded) {
      // not succeeded, throw out exception
      if (exceptions == null || exceptions.isEmpty()) {
        throw new RssInvalidStateException("Invalid read client state: failed to initialized, but no exceptions");
      } else {
        throw new RssAggregateException(exceptions.stream().map(t->t.exception).collect(Collectors.toList()));
      }
    } else {
      // succeeded, log exceptions as warning
      if (exceptions != null) {
        exceptions.forEach(t -> {
          logger.warn(t.logMsg, t.exception);
        });
      }
    }
  }

  private void rememberLastConsumedRecord(RecordKeyValuePair record) {
    increaseRecordCount(numConsumedRecordsMap, record.getTaskAttemptId());
    if (checkDataConsistency) {
      lastConsumedRecordsMap.put(record.getTaskAttemptId(), record);
    }
  }

  private void rememberLastReadRecord(RecordKeyValuePair record) {
    increaseRecordCount(numReadRecordsMap, record.getTaskAttemptId());
    if (checkDataConsistency) {
      lastReadRecordsMap.put(record.getTaskAttemptId(), record);
    }
  }

  private void resetReadRecords() {
    numReadRecordsMap.clear();
    lastReadRecordsMap.clear();
  }

  private void increaseRecordCount(Map<Long, Long> recordCountMap, long taskAttemptId) {
    try {
      long oldValue = recordCountMap.getOrDefault(taskAttemptId, 0L);
      recordCountMap.put(taskAttemptId, oldValue + 1);
    } catch (Throwable ex) {
      throw new RssNonRecoverableException(String.format("Failed to increase number of read records for task attempt %s, %s", taskAttemptId, this), ex);
    }
  }

  private boolean shouldSkipReadRecord(RecordKeyValuePair record) {
    long taskAttemptId = record.getTaskAttemptId();
    Long consumedCount = numConsumedRecordsMap.get(taskAttemptId);
    if (consumedCount == null) {
      return false;
    }

    long numReadCount = numReadRecordsMap.getOrDefault(taskAttemptId, 0L);
    if (numReadCount < consumedCount) {
      return true;
    } else if (numReadCount == consumedCount) {
      if (checkDataConsistency) {
        RecordKeyValuePair lastConsumedRecord = lastConsumedRecordsMap.get(taskAttemptId);
        RecordKeyValuePair lastReadRecord = lastReadRecordsMap.get(taskAttemptId);
        if (!recordEquals(lastConsumedRecord, lastReadRecord)) {
          throw new RssInconsistentReplicaException(
              String.format("Got different records from two servers in the replication group for task attempt %s (after %s records), record from previous server: %s, record from new server: %s (%s)",
                  taskAttemptId, numReadCount, lastConsumedRecord, lastReadRecord, clients[currentClientIndex])
          );
        }
      }
      return false;
    } else {
      throw new RssInconsistentReplicaException(String.format(
          "Inconsistent replica for task attempt %s, consumed %s records, read %s records, current client/server: %s",
          taskAttemptId,
          consumedCount,
          numReadCount,
          clients[currentClientIndex]));
    }
  }

  private void checkRecordDataConsistency() {
    MapDifference<Long, Long>  numRecordsDifference = Maps.difference(numReadRecordsMap, numConsumedRecordsMap);
    if (!numRecordsDifference.areEqual()) {
      throw new RssInconsistentReplicaException(String.format(
          "Data corrupted! Number of consumed records (returned to caller): %s. Number of records read from current server: %s ((%s))",
          numConsumedRecordsMap,
          numReadRecordsMap,
          clients[currentClientIndex]));
    }
    if (checkDataConsistency) {
      MapDifference<Long, RecordKeyValuePair> lastRecordsDifference = Maps.difference(lastReadRecordsMap, lastConsumedRecordsMap);
      if (!lastRecordsDifference.areEqual()) {
        // TODO refine exception message
        throw new RssInconsistentReplicaException(String.format(
            "Data corrupted! Last consumed records (returned to caller): %s. Last records read from current server: %s ((%s))",
            lastConsumedRecordsMap,
            lastReadRecordsMap,
            clients[currentClientIndex]));
      }
    }
  }

  private RetriableSocketReadClient getActiveClient() {
    if (currentClientIndex > clients.length - 1) {
      throw new RssNoActiveReadClientException("No active read client for server replication group: " + serverReplicationGroup);
    }
    return clients[currentClientIndex];
  }

  private void closeClient(int clientIndex) {
    closeClient(clients[clientIndex]);
    clientsInitialized[clientIndex] = false;
    resetReadRecords();
  }

  private void closeClient(RecordReader client) {
    try {
      client.close();
    } catch (Throwable ex) {
      logger.warn("Failed to close read client: " + client, ex);
    }
  }

  private boolean recordEquals(RecordKeyValuePair record1, RecordKeyValuePair record2) {
    return Objects.equals(record1, record2);
  }

  private static class ExceptionLogInfo {
    private String logMsg;
    private Throwable exception;

    public ExceptionLogInfo(String logMsg, Throwable exception) {
      this.logMsg = logMsg;
      this.exception = exception;
    }
  }
}
