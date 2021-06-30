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

import com.uber.rss.common.ServerDetail;
import com.uber.rss.util.NetworkUtils;
import com.uber.rss.util.ServerHostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MultiServerHeartbeatClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MultiServerHeartbeatClient.class);

  private static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(30);
  private static final long DEFAULT_NETWORK_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1);

  private static final MultiServerHeartbeatClient instance = new MultiServerHeartbeatClient(DEFAULT_HEARTBEAT_INTERVAL_MILLIS, DEFAULT_NETWORK_TIMEOUT_MILLIS);

  private static final ServerConnectionStringCache serverCache = new ServerConnectionStringCache();

  public static MultiServerHeartbeatClient getInstance() {
    return instance;
  }

  public static ServerConnectionStringCache getServerCache() {
    return serverCache;
  }

  private final long networkTimeoutMillis;
  private final ConcurrentHashMap<String, ServerDetail> servers = new ConcurrentHashMap<>();

  private volatile boolean keepRunning = true;

  private volatile String user;
  private volatile String appId;
  private volatile String appAttempt;

  private volatile ServerConnectionRefresher serverConnectionRefresher;

  public MultiServerHeartbeatClient(long heartbeatIntervalMillis, long networkTimeoutMillis) {
    this.networkTimeoutMillis = networkTimeoutMillis;

    Thread thread = new Thread(() -> {
      while (keepRunning) {
        try {
          sendHeartbeats();
        } catch (Throwable e) {
          logger.warn("Failed to send heartbeats", e);
        }

        try {
          Thread.sleep(heartbeatIntervalMillis);
        } catch (Throwable e) {
          logger.info("RSS Heartbeat thread got interrupted", e);
        }
      }
    });

    thread.setName("RSS_Heartbeat_" + thread.hashCode());
    thread.setDaemon(true);
    thread.start();
    logger.info("Started RSS heartbeat thread {} with interval {} millis", thread, heartbeatIntervalMillis);
  }

  public void setAppContext(String user, String appId, String appAttempt) {
    this.user = user;
    this.appId = appId;
    this.appAttempt = appAttempt;
  }

  public boolean hasServerConnectionRefresher() {
    return this.serverConnectionRefresher != null;
  }

  public void setServerConnectionRefresher(ServerConnectionRefresher serverConnectionRefresher) {
    this.serverConnectionRefresher = serverConnectionRefresher;
  }

  public void addServers(Collection<ServerDetail> serverDetails) {
    for (ServerDetail s: serverDetails) {
      addServer(s);
    }
  }

  public void clearServers() {
    servers.clear();
  }

  public void addServer(ServerDetail serverDetail) {
    ServerDetail oldServerDetail = servers.get(serverDetail.getServerId());
    if (oldServerDetail == null || oldServerDetail.getRunningVersionAsNumber() < serverDetail.getRunningVersionAsNumber()) {
      servers.put(serverDetail.getServerId(), serverDetail);
    }
  }

  public void sendHeartbeats() {
    if (appId == null) {
      return;
    }

    List<ServerDetail> serverDetails = new ArrayList<>(servers.values());
    if (serverDetails.size() == 0) {
      return;
    }

    for (ServerDetail serverDetail: serverDetails) {
      try {
        sendHeartbeat(serverDetail);
      } catch (Throwable ex) {
        logger.warn(String.format("Failed to send RSS heartbeat to %s", serverDetail), ex);
      }
    }
  }

  private void sendHeartbeat(ServerDetail serverDetail) {
    ServerHostAndPort hostAndPort = ServerHostAndPort.fromString(serverDetail.getConnectionString());
    long startTime = System.currentTimeMillis();
    boolean keepLive = false;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(hostAndPort.getHost(), hostAndPort.getPort(), (int)networkTimeoutMillis, user, appId, appAttempt, keepLive)) {
      client.sendHeartbeat();
      logger.info("Sent RSS heartbeat to {}, duration millis: {}", serverDetail, System.currentTimeMillis() - startTime);
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to send RSS heartbeat to %s", serverDetail), ex);
      if (serverConnectionRefresher != null) {
        ServerDetail refreshedServerDetail = serverConnectionRefresher.refreshConnection(serverDetail);
        if (refreshedServerDetail != null) {
          addServer(refreshedServerDetail);
        }
      }
    }
  }

  @Override
  public void close() {
    keepRunning = false;
    clearServers();
  }
}
