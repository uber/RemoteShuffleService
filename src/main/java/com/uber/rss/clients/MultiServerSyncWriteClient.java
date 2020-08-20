package com.uber.rss.clients;

import com.google.common.util.concurrent.Uninterruptibles;
import com.uber.m3.tally.Stopwatch;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.WriteClientMetrics;
import com.uber.rss.metrics.WriteClientMetricsKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/***
 * This write client writes records to different shuffle servers based on its partition.
 * This class is not thread safe and should be only called in same thread.
 */
public class MultiServerSyncWriteClient implements MultiServerWriteClient {
    private static final Logger logger = LoggerFactory.getLogger(MultiServerSyncWriteClient.class);

    private final List<ServerConnectionInfo> servers = new ArrayList<>();
    private final int networkTimeoutMillis;
    private final ServerConnectionRefresher serverConnectionRefresher;
    private final boolean finishUploadAck;
    private final boolean usePooledConnection;
    private final int compressionBufferSize;
    private final String user;
    private final String appId;
    private final String appAttempt;
    private final long maxTryingMillis;
    private final ShuffleWriteConfig shuffleWriteConfig;

    private final ReplicatedWriteClient[] clients;

    private final WriteClientMetrics metrics;

    private final int partitionFanout;

    private long taskAttemptId;

    public MultiServerSyncWriteClient(Collection<ServerReplicationGroup> servers, int networkTimeoutMillis, long maxTryingMillis, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
        this(servers, 1, networkTimeoutMillis, maxTryingMillis, null, finishUploadAck, usePooledConnection, compressionBufferSize, user, appId, appAttempt, shuffleWriteConfig);
    }

    @SuppressWarnings("unchecked")
    public MultiServerSyncWriteClient(Collection<ServerReplicationGroup> servers, int partitionFanout, int networkTimeoutMillis, long maxTryingMillis, ServerConnectionRefresher serverConnectionRefresher, boolean finishUploadAck, boolean usePooledConnection, int compressionBufferSize, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
        for (ServerReplicationGroup entry: servers) {
            this.servers.add(new ServerConnectionInfo(this.servers.size(), entry));
        }
        this.partitionFanout = partitionFanout;
        this.networkTimeoutMillis = networkTimeoutMillis;
        this.maxTryingMillis = maxTryingMillis;
        this.serverConnectionRefresher = serverConnectionRefresher;
        this.finishUploadAck = finishUploadAck;
        this.usePooledConnection = usePooledConnection;
        this.compressionBufferSize = compressionBufferSize;
        this.user = user;
        this.appId = appId;
        this.appAttempt = appAttempt;
        this.shuffleWriteConfig = shuffleWriteConfig;
        this.clients = new ReplicatedWriteClient[this.servers.size()];

        this.metrics = new WriteClientMetrics(new WriteClientMetricsKey(
            this.getClass().getSimpleName(), user));
        metrics.getNumClients().inc(1);

        if (partitionFanout > this.servers.size()) {
            throw new RssInvalidDataException(String.format(
                "Too many servers (%s) per partition, larger than max number of servers (%s)",
                partitionFanout,
                this.servers.size()));
        }

        logger.info(String.format("Created %s", this.getClass().getSimpleName()));
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
    }

    @Override
    public void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
        taskAttemptId = appTaskAttemptId.getTaskAttemptId();
        Arrays.stream(clients).forEach(t->t.startUpload(appTaskAttemptId, numMaps, numPartitions));
    }

    @Override
    public void sendRecord(int partition, ByteBuffer key, ByteBuffer value) {
        int clientIndex = partition % clients.length;
        if (partitionFanout > 1) {
            clientIndex = ((int)(clientIndex + taskAttemptId % partitionFanout)) % clients.length;
        }
        ReplicatedWriteClient writeClient = clients[clientIndex];
        writeClient.sendRecord(partition, key, value);
    }

    @Override
    public void finishUpload() {
        Stopwatch stopwatch = metrics.getFinishUploadLatency().start();
        try {
            Arrays.stream(clients).parallel().forEach(ReplicatedWriteClient::finishUpload);
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
        closeMetrics();

        Arrays.stream(clients).parallel().forEach(t -> closeClient(t));
    }

    @Override
    public String toString() {
        return "MultiServerSyncWriteClient{" +
            "clients=" + Arrays.toString(clients) +
            '}';
    }

    private void connectSingleClient(ServerConnectionInfo server) {
        final long startTime = System.currentTimeMillis();
        ReplicatedWriteClient client = new ReplicatedWriteClient(
            server.server, networkTimeoutMillis, serverConnectionRefresher, finishUploadAck, usePooledConnection, compressionBufferSize, user, appId, appAttempt, shuffleWriteConfig);
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
                logger.debug(String.format("Closing client: %s", client));
                client.close();
            }
        } catch (Throwable ex) {
            logger.warn("Failed to close client", ex);
        }
    }

    private void closeMetrics() {
        try {
            metrics.close();
        } catch (Throwable e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            logger.warn(String.format("Failed to close metrics: %s", this), e);
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

}
