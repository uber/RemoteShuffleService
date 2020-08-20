package com.uber.rss.clients;

import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerReplicationGroup;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class write same shuffle data to multiple shuffle servers (replication group) to achieve fault tolerance.
 */
public class ReplicatedWriteClient implements MultiServerWriteClient {
  private static final Logger logger = LoggerFactory.getLogger(ReplicatedWriteClient.class);

  private final ServerReplicationGroup serverReplicationGroup;
  private final ServerIdAwareSyncWriteClient[] clients;

  private long shuffleWriteBytes = -1;

  public ReplicatedWriteClient(ServerReplicationGroup serverReplicationGroup, int timeoutMillis, boolean finishUploadAck, boolean usePooledConnection, int compressBufferSize, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
    this(serverReplicationGroup, timeoutMillis, null, finishUploadAck, usePooledConnection, compressBufferSize,user, appId, appAttempt, shuffleWriteConfig);
  }

  public ReplicatedWriteClient(ServerReplicationGroup serverReplicationGroup, int timeoutMillis, ServerConnectionRefresher serverConnectionRefresher, boolean finishUploadAck, boolean usePooledConnection, int compressBufferSize, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
    this.serverReplicationGroup = serverReplicationGroup;

    List<ServerDetail> servers = serverReplicationGroup.getServers();
    if (servers.isEmpty()) {
      throw new RssException("No server in replication group");
    }

    clients = new ServerIdAwareSyncWriteClient[servers.size()];
    for (int i = 0; i < servers.size(); i++) {
      ServerDetail serverDetail = servers.get(i);
      ServerIdAwareSyncWriteClient client = new ServerIdAwareSyncWriteClient(serverDetail, timeoutMillis, finishUploadAck, usePooledConnection, compressBufferSize, user, appId, appAttempt, shuffleWriteConfig, serverConnectionRefresher);
      clients[i] = client;
    }
  }

  @Override
  public synchronized void connect() {
    runAllActiveClients(t->t.connect());
  }

  @Override
  public synchronized void startUpload(AppTaskAttemptId appTaskAttemptId, int numMaps, int numPartitions) {
    runAllActiveClients(t->t.startUpload(appTaskAttemptId, numMaps, numPartitions));
  }

  @Override
  public synchronized void sendRecord(int partition, ByteBuffer key, ByteBuffer value) {
    if (key != null) {
      key.mark();
    }
    if (value != null) {
      value.mark();
    }
    runAllActiveClients(t-> {
      if (key != null) {
        key.reset();
      }
      if (value != null) {
        value.reset();
      }
      t.sendRecord(partition, key, value);
    });
  }

  @Override
  public synchronized void finishUpload() {
    runAllActiveClients(t->t.finishUpload());
  }

  @Override
  public synchronized long getShuffleWriteBytes() {
    if (shuffleWriteBytes >= 0) {
      return shuffleWriteBytes;
    }

    long shuffleWriteBytes = -1;
    String shuffleWriteBytesClientInfo = null;

    for (int i = 0; i < clients.length; i++) {
      ServerIdAwareSyncWriteClient client = clients[i];
      if (client != null) {
        long currentClientValue = client.getShuffleWriteBytes();
        if (shuffleWriteBytes == -1) {
          shuffleWriteBytes = currentClientValue;
          shuffleWriteBytesClientInfo = client.toString();
        } else if (shuffleWriteBytes != currentClientValue) {
          throw new RssInvalidStateException(String.format(
              "Inconsistent state, client (%s) wrote %s shuffle bytes, while another client (%s) wrote %s shuffle bytes",
              shuffleWriteBytesClientInfo, shuffleWriteBytes, client, currentClientValue));
        }
      }
    }

    if (shuffleWriteBytes == -1) {
      throw new RssException("No active client with server replication group: " + serverReplicationGroup);
    }

    return shuffleWriteBytes;
  }

  @Override
  public synchronized void close() {
    if (!hasActiveClient()) {
      return;
    }

    // remember shuffle write bytes because we may not get it back after closing the client
    shuffleWriteBytes = getShuffleWriteBytes();

    runAllActiveClients(t->t.close());

    for (int i = 0; i < clients.length; i++) {
      clients[i] = null;
    }
  }

  @Override
  public String toString() {
    return "ReplicatedWriteClient{" +
        "clients=" + Arrays.toString(clients) +
        '}';
  }

  private void runAllActiveClients(Consumer<ServerIdAwareSyncWriteClient> action) {
    Exception exception = null;
    boolean succeeded = false;
    int numActiveClients = 0;
    for (int i = 0; i < clients.length; i++) {
      ServerIdAwareSyncWriteClient client = clients[i];
      if (client != null) {
        numActiveClients++;
        try {
          action.accept(client);
          succeeded = true;
        } catch (Exception ex) {
          exception = ex;
          M3Stats.addException(ex, this.getClass().getSimpleName());
          logger.warn("Failed to run client: " + client, ex);

          clients[i] = null;
          try {
            client.close();
          } catch (Throwable closeException) {
            logger.warn("Failed to close client: " + client, closeException);
          }
        }
      }
    }

    if (numActiveClients == 0) {
      throw new RssException("No active client connecting to server replication group: " + serverReplicationGroup);
    }

    if (!succeeded) {
      if (exception == null) {
        throw new RssInvalidStateException(String.format("No underlying client succeeded, but no exception as well, %s", this));
      }
      ExceptionUtils.throwException(exception);
    }
  }

  private boolean hasActiveClient() {
    for (ServerIdAwareSyncWriteClient client: clients) {
      if (client != null) {
        return true;
      }
    }
    return false;
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
