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

import com.uber.m3.tally.Stopwatch;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssQueueNotReadyException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.WriteClientMetrics;
import com.uber.rss.metrics.WriteClientMetricsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/***
 * This write client uses background threads to send records. All records will be stored in blocking queues.
 * The background threads will poll the queues to get records and send to servers. Each record will be sent
 * to a server replication group and written to all servers in that group.
 * This class is not thread safe and should be only called in same thread.
 */
public class MultiServerAsyncWriteClient implements MultiServerWriteClient {
    private static final Logger logger = LoggerFactory.getLogger(MultiServerAsyncWriteClient.class);

    private final List<ServerConnectionInfo> servers = new ArrayList<>();
    private final int networkTimeoutMillis;
    private final long maxTryingMillis;
    private final ServerConnectionRefresher serverConnectionRefresher;
    private final boolean finishUploadAck;
    private final boolean usePooledConnection;
    private final String user;
    private final String appId;
    private final String appAttempt;
    private final ShuffleWriteConfig shuffleWriteConfig;

    private final ReplicatedWriteClient[] clients;

    private final BlockingQueue<Record>[] recordQueues;
    private final Thread[] threads;

    private long lastLogTime = System.currentTimeMillis();
    private final long logInterval = 30000;

    private final AtomicLong queueInsertTime = new AtomicLong();
    private final AtomicLong queuePollTime = new AtomicLong();
    private final AtomicLong socketTime = new AtomicLong();

    private final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();

    private boolean threadStarted = false;
    private AppTaskAttemptId currentAppTaskAttemptId;

    private final WriteClientMetrics metrics;

    private final int partitionFanout;

    public MultiServerAsyncWriteClient(Collection<ServerReplicationGroup> servers, int networkTimeoutMillis, long maxTryingMillis, boolean finishUploadAck, boolean usePooledConnection, int writeQueueSize, int numThreads, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
        this(servers, 1, networkTimeoutMillis, maxTryingMillis, null, finishUploadAck, usePooledConnection, writeQueueSize, numThreads, user, appId, appAttempt, shuffleWriteConfig);
    }

    @SuppressWarnings("unchecked")
    public MultiServerAsyncWriteClient(Collection<ServerReplicationGroup> servers, int partitionFanout, int networkTimeoutMillis, long maxTryingMillis, ServerConnectionRefresher serverConnectionRefresher, boolean finishUploadAck, boolean usePooledConnection, int writeQueueSize, int numThreads, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
        for (ServerReplicationGroup entry: servers) {
            this.servers.add(new ServerConnectionInfo(this.servers.size(), entry));
        }
        this.partitionFanout = partitionFanout;
        this.networkTimeoutMillis = networkTimeoutMillis;
        this.maxTryingMillis = maxTryingMillis;
        this.serverConnectionRefresher = serverConnectionRefresher;
        this.finishUploadAck = finishUploadAck;
        this.usePooledConnection = usePooledConnection;
        this.user = user;
        this.appId = appId;
        this.appAttempt = appAttempt;
        this.shuffleWriteConfig = shuffleWriteConfig;
        this.clients = new ReplicatedWriteClient[this.servers.size()];
        this.recordQueues = (BlockingQueue<Record>[])Array.newInstance(ArrayBlockingQueue.class, numThreads);
        for (int i = 0; i < numThreads; i++) {
            this.recordQueues[i] = new ArrayBlockingQueue<>(writeQueueSize);
        }
        this.threads = new Thread[numThreads];

        this.metrics = new WriteClientMetrics(new WriteClientMetricsKey(
            this.getClass().getSimpleName(), user));
        metrics.getNumClients().inc(1);

        if (partitionFanout > this.servers.size()) {
            throw new RssInvalidDataException(String.format(
                "Too many servers (%s) per partition, larger than max number of servers (%s)",
                partitionFanout,
                this.servers.size()));
        }

        logger.info("Created {}, threads: {}, queue size: {}", this.getClass().getSimpleName(), numThreads, writeQueueSize);
    }

    @Override
    public void connect() {
        servers.parallelStream().forEach(t -> connectSingleClient(t));

        // use synchronize to make sure reads on clients array element getting latest value from other threads
        // see http://www.cs.umd.edu/~pugh/java/memoryModel/jsr-133-faq.html
        synchronized (clients) {
            // sanity check that clients are initialized correctly
            for (int i = 0; i < clients.length; i++) {
                if (clients[i] == null) {
                    throw new RssInvalidStateException(String.format("Client %s is null", i));
                }
            }
        }

        for (int i = 0; i < threads.length; i++) {
            final int threadIndex = i;
            Thread thread = new Thread(() -> {
                logger.info("Record Thread {} started", threadIndex);
                BlockingQueue<Record> recordQueue = recordQueues[threadIndex];
                try {
                    // TODO optimize the max wait time for poll
                    long pollMaxWait = networkTimeoutMillis * 4;
                    while (exceptions.isEmpty()) {
                        long startTime = System.nanoTime();
                        // TODO optimize here to restart thread if there is new record?
                        Record record = recordQueue.poll(pollMaxWait, TimeUnit.MILLISECONDS);
                        queuePollTime.addAndGet(System.nanoTime() - startTime);
                        if (record != null) {
                            if (record.isStopMarker) {
                                break;
                            }
                            ReplicatedWriteClient writeClient = clients[record.clientIndex];
                            startTime = System.nanoTime();
                            writeClient.writeDataBlock(record.partition, record.value);
                            socketTime.addAndGet(System.nanoTime() - startTime);
                        } else {
                            logger.info("Record queue {} has no record after waiting {} millis", threadIndex, pollMaxWait);
                        }
                    }
                } catch (Throwable e) {
                    logger.warn("Record Thread {} got exception", threadIndex, e);
                    M3Stats.addException(e, this.getClass().getSimpleName());
                    exceptions.add(e);
                }
                int remainingRecords = recordQueue.size();
                if (remainingRecords > 0) {
                    exceptions.add(new RssQueueNotReadyException(String.format("Record queue %s has %s remaining records not sent out", threadIndex, remainingRecords)));
                }
                recordQueue.clear();
                logger.info("Record Thread {} finished, remaining records: {}", threadIndex, remainingRecords);
            });
            thread.setName("Record Thread " + i);
            threads[threadIndex] = thread;
        }
    }

    @Override
    public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
        currentAppTaskAttemptId = appTaskAttemptId;
        Arrays.stream(clients).forEach(t->t.startUpload(appTaskAttemptId, numMaps, numPartitions));
    }

    @Override
    public void writeDataBlock(int partition, ByteBuffer value) {
        if (!threadStarted) {
            for (Thread thread: threads) {
                thread.start();
            }
            threadStarted = true;
        }

        if (!exceptions.isEmpty()) {
            throw new RssAggregateException(exceptions);
        }

        int clientIndex = partition % clients.length;
        if (partitionFanout > 1) {
            clientIndex = (clientIndex + (int)(currentAppTaskAttemptId.getTaskAttemptId() % partitionFanout)) % clients.length;
        }

        int threadIndex = clientIndex % threads.length;
        BlockingQueue<Record> recordQueue = recordQueues[threadIndex];
        try {
            long startTime = System.nanoTime();
            boolean inserted = recordQueue.offer(createUploadRecord(partition, value, clientIndex), networkTimeoutMillis, TimeUnit.MILLISECONDS);
            queueInsertTime.addAndGet(System.nanoTime() - startTime);
            if (!inserted) {
                throw new RssQueueNotReadyException(String.format("sendRecord: Record queue has no space available after waiting %s millis", networkTimeoutMillis));
            }
        } catch (InterruptedException e) {
            throw new RssException("Interrupted when inserting to record queue", e);
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLogTime > logInterval) {
            for (int i = 0; i < recordQueues.length; i++) {
                logger.info("Record queue {} size: {}", i, recordQueues[i].size());
            }
            lastLogTime = currentTime;
        }
    }

    @Override
    public void finishUpload() {
        Stopwatch stopwatch = metrics.getFinishUploadLatency().start();
        try {
            long stopThreadStartTime = System.nanoTime();

            stopThreads();
            waitThreadsExit();

            long stopThreadTime = System.nanoTime() - stopThreadStartTime;

            if (!exceptions.isEmpty()) {
                throw new RssAggregateException(exceptions);
            }

            long underlyingClientFinishUploadStartTime = System.nanoTime();

            Arrays.stream(clients).parallel().forEach(ReplicatedWriteClient::finishUpload);

            long underlyingClientFinishUploadTime = System.nanoTime() - underlyingClientFinishUploadStartTime;

            logger.info("WriteClientTime ({}), queue insert seconds: {}, queue poll seconds: {}, socket seconds: {}, stop thread seconds: {}, finish upload seconds: {}",
                currentAppTaskAttemptId,
                TimeUnit.NANOSECONDS.toSeconds(queueInsertTime.get()),
                TimeUnit.NANOSECONDS.toSeconds(queuePollTime.get()),
                TimeUnit.NANOSECONDS.toSeconds(socketTime.get()),
                TimeUnit.NANOSECONDS.toSeconds(stopThreadTime),
                TimeUnit.NANOSECONDS.toSeconds(underlyingClientFinishUploadTime));
        } finally {
            stopwatch.stop();
        }
    }

    @Override
    public long getShuffleWriteBytes() {
        long result = 0;
        for (ReplicatedWriteClient entry: clients) {
            if (entry != null) {
                result += entry.getShuffleWriteBytes();
            }
        }
        return result;
    }

    @Override
    public void close() {
        try {
            stopThreads();

            closeMetrics();

            waitThreadsExit();
        } finally {
            Arrays.stream(clients).parallel().forEach(t -> closeClient(t));
        }

        if (!exceptions.isEmpty()) {
            throw new RssAggregateException(exceptions);
        }
    }

    @Override
    public String toString() {
        return "MultiServerAsyncWriteClient{" +
            "clients=" + Arrays.toString(clients) +
            '}';
    }

    private void connectSingleClient(ServerConnectionInfo server) {
        ReplicatedWriteClient client = new ReplicatedWriteClient(
            server.server, networkTimeoutMillis, serverConnectionRefresher, finishUploadAck, usePooledConnection, user, appId, appAttempt, shuffleWriteConfig);
        client.connect();

        // use synchronize to make sure writes on clients array element visible to other threads
        // see http://www.cs.umd.edu/~pugh/java/memoryModel/jsr-133-faq.html
        synchronized (clients) {
            clients[server.index] = client;
        }
    }

    private void closeClient(ReplicatedWriteClient client) {
        try {
            if (client != null) {
                logger.debug("Closing client: {}", client);
                client.close();
            }
        } catch (Throwable ex) {
            logger.warn("Failed to close client", ex);
        }
    }

    private void stopThreads() {
        if (!threadStarted) {
            return;
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                boolean inserted = recordQueues[i].offer(createStopMarkerRecord(), networkTimeoutMillis, TimeUnit.MILLISECONDS);
                if (!inserted) {
                    throw new RssQueueNotReadyException(String.format("stopThreads: Record queue has no space available after waiting %s millis", networkTimeoutMillis));
                }
                logger.debug("Inserted stop marker to record queue {}", i);
            } catch (InterruptedException e) {
                throw new RssException("Interrupted when inserting stop marker to record queue", e);
            }
        }
    }

    private void waitThreadsExit() {
        if (!threadStarted) {
            return;
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                Thread thread = threads[i];
                if (thread != null) {
                    thread.join(maxTryingMillis);

                    if (thread.isAlive()) {
                        exceptions.add(new RssException(String.format("Thread %s still alive after waiting %s milliseconds", i, maxTryingMillis)));
                    }
                }
            } catch (InterruptedException e) {
                throw new RssException(String.format("Failed to wait record thread %s exit", i), e);
            }
        }

        threadStarted = false;
    }

    private Record createUploadRecord(int partition, ByteBuffer value, int clientIndex) {
        return new Record(partition, value, clientIndex);
    }

    private Record createStopMarkerRecord() {
        return new Record(true);
    }

    private void closeMetrics() {
        try {
            metrics.close();
        } catch (Throwable e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            logger.warn("Failed to close metrics: {}", this, e);
        }
    }

    private static class ServerConnectionInfo {
        private int index;
        private ServerReplicationGroup server;

        public ServerConnectionInfo(int index, ServerReplicationGroup server) {
            this.index = index;
            this.server = server;
        }

        @Override
        public String toString() {
            return "ServerConnectionInfo{" +
                "index=" + index +
                ", server=" + server +
                '}';
        }
    }

    private static class Record {
        private boolean isStopMarker = false;

        private int partition;
        private ByteBuffer value;

        private int clientIndex;

        public Record(int partition, ByteBuffer value, int clientIndex) {
            this.partition = partition;
            this.value = value;
            this.clientIndex = clientIndex;
        }

        public Record(boolean isStopMarker) {
            this.isStopMarker = isStopMarker;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "partition=" + partition +
                    '}';
        }
    }
}
