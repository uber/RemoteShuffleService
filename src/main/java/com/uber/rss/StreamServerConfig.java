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

package com.uber.rss;

import com.uber.rss.clients.ClientConstants;
import com.uber.rss.common.Compression;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.metadata.ServiceRegistry;
import com.uber.rss.metadata.ZooKeeperServiceRegistry;
import com.uber.rss.storage.ShuffleFileStorage;
import com.uber.rss.storage.ShuffleStorage;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class StreamServerConfig {
  public static final long DEFAULT_SERVER_SIDE_CONNECTION_IDLE_TIMEOUT_MILLIS = 60 * 60 * 1000;

  public static final String DEFAULT_DATA_CENTER = "dataCenter1";

  private boolean useEpoll = false;

  private int shufflePort = 19190;

  private int httpPort = -1;

  private String rootDir = "";

  // number of threads for netty to accept incoming socket
  private int nettyAcceptThreads = 2;

  // number of threads for netty to process socket data
  private int nettyWorkerThreads = 20;

  private int networkBacklog = 1000;

  private int networkTimeout = 30000;

  private int networkRetries = 5;

  private ShuffleStorage storage = new ShuffleFileStorage();

  private boolean isFsyncEnabled = true;

  private int throttleMemoryPercentage = 80;

  private long maxUploadPauseMillis = 1000;

  private boolean isDaemonExecutorThread = false;

  private int numSplits = 1;

  // How long milliseconds before clean up each application after its last liveness update
  private long appMemoryRetentionMillis = ShuffleExecutor.DEFAULT_APP_MEMORY_RETENTION_MILLIS;

  // How long milliseconds to clean up application shuffle files older than that
  private long appFileRetentionMillis = ShuffleExecutor.DEFAULT_APP_FILE_RETENTION_MILLIS;

  private String serviceRegistryType = ServiceRegistry.TYPE_INMEMORY;

  private String dataCenter;

  private String cluster = ServiceRegistry.DEFAULT_TEST_CLUSTER;

  private String zooKeeperServers = ZooKeeperServiceRegistry.getDefaultServers();

  private String zooKeeperServersBackup = null;

  private String registryServer = null;

  private int maxConnections = 40000;

  private int bufferSize = ShuffleFileStorage.DEFAULT_BUFFER_SIZE;

  private String networkCompressionCodec = "";

  private String fileCompressionCodec = Compression.COMPRESSION_CODEC_LZ4;

  // use client side idle timeout plus one extra minute as server side idle timeout. so client could close idle connection
  // before server side closes it. also make sure that the timeout is at least as long as
  // DEFAULT_SERVER_SIDE_CONNECTION_IDLE_TIMEOUT_MILLIS.
  private long idleTimeoutMillis = Math.max(
      ClientConstants.DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS + TimeUnit.MINUTES.toMillis(1),
      DEFAULT_SERVER_SIDE_CONNECTION_IDLE_TIMEOUT_MILLIS);

  private long appMaxWriteBytes = ShuffleExecutor.DEFAULT_APP_MAX_WRITE_BYTES;

  private String keytab = null;

  private String principal = null;

  private Configuration hadoopConfig = null;

  private long stateCommitIntervalMillis = 60000;

  private boolean isJFxDebugProfilerEnable = true;

  public static StreamServerConfig buildFromArgs(String[] args) throws IOException {
    StreamServerConfig serverConfig = new StreamServerConfig();

    String hadoopConfFiles = null;

    // TODO use library to process arguments
    for (int i = 0; i < args.length; ) {
      String argName = args[i++];
      if (argName.equalsIgnoreCase("-epoll")) {
        serverConfig.useEpoll = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-port")) {
        serverConfig.shufflePort = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-httpPort")) {
        serverConfig.httpPort = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-rootDir")) {
        serverConfig.rootDir = args[i++];
      } else if (argName.equalsIgnoreCase("-nettyAcceptThreads")) {
        serverConfig.nettyAcceptThreads = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-nettyWorkerThreads")) {
        serverConfig.nettyWorkerThreads = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-networkBacklog")) {
        serverConfig.networkBacklog = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-networkTimeout")) {
        serverConfig.networkTimeout = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-networkRetries")) {
        serverConfig.networkRetries = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-fsync")) {
        serverConfig.isFsyncEnabled = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-jfxProfiler")) {
        serverConfig.isJFxDebugProfilerEnable = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-bufferSize")) {
        serverConfig.bufferSize = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-appMemoryRetentionMillis")) {
        serverConfig.appMemoryRetentionMillis = Long.parseLong(args[i++]);
      } else if (argName.equalsIgnoreCase("-appFileRetentionMillis")) {
        serverConfig.appFileRetentionMillis = Long.parseLong(args[i++]);
      } else if (argName.equalsIgnoreCase("-appMaxWriteBytes")) {
        serverConfig.appMaxWriteBytes = Long.parseLong(args[i++]);
      } else if (argName.equalsIgnoreCase("-hadoopConf")) {
        hadoopConfFiles = args[i++];
      } else if (argName.equalsIgnoreCase("-keytab")) {
        serverConfig.keytab = args[i++];
      } else if (argName.equalsIgnoreCase("-principal")) {
        serverConfig.principal = args[i++];
      } else if (argName.equalsIgnoreCase("-memoryPercentage")) {
        serverConfig.throttleMemoryPercentage = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-daemonThread")) {
        serverConfig.isDaemonExecutorThread = Boolean.parseBoolean(args[i++]);
      } else if (argName.equalsIgnoreCase("-splits")) {
        serverConfig.numSplits = Integer.parseInt(args[i++]);
      } else if (argName.equalsIgnoreCase("-serviceRegistry")) {
        serverConfig.serviceRegistryType = args[i++].toLowerCase();
      } else if (argName.equalsIgnoreCase("-dataCenter")) {
        serverConfig.dataCenter = args[i++];
      } else if (argName.equalsIgnoreCase("-cluster")) {
        serverConfig.cluster = args[i++];
      } else if (argName.equalsIgnoreCase("-zooKeeperServers")) {
        serverConfig.zooKeeperServers = args[i++];
      } else if (argName.equalsIgnoreCase("-zooKeeperServersBackup")) {
        serverConfig.zooKeeperServersBackup = args[i++];
      } else if (argName.equalsIgnoreCase("-registryServer")) {
        serverConfig.registryServer = args[i++];
      } else if (argName.equalsIgnoreCase("-networkCompressionCodec")) {
        serverConfig.networkCompressionCodec = args[i++];
      } else if (argName.equalsIgnoreCase("-fileCompressionCodec")) {
        serverConfig.fileCompressionCodec = args[i++];
      } else if (argName.equalsIgnoreCase("-stateCommitIntervalMillis")) {
        serverConfig.stateCommitIntervalMillis = Long.parseLong(args[i++]);
      } else {
        throw new IllegalArgumentException("Unsupported argument: " + argName);
      }
    }

    if (!ServiceRegistry.VALID_TYPES.contains(serverConfig.serviceRegistryType)) {
      throw new IllegalArgumentException("Unsupported value for -serviceRegistry: " + serverConfig.serviceRegistryType);
    }

    if (serverConfig.rootDir.isEmpty()) {
      serverConfig.rootDir = Files.createTempDirectory("StreamServer_").toString();
    }

    serverConfig.storage = new ShuffleFileStorage(serverConfig.bufferSize);

    return serverConfig;
  }

  public boolean isUseEpoll() {
    return useEpoll;
  }

  public void setUseEpoll(boolean useEpoll) {
    this.useEpoll = useEpoll;
  }

  public int getShufflePort() {
    return shufflePort;
  }

  public void setShufflePort(int port) {
    shufflePort = port;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int port) {
    httpPort = port;
  }

  public String getRootDirectory() {
    return rootDir;
  }

  public void setRootDirectory(String dir) {
    rootDir = dir;
  }

  public int getNettyAcceptThreads() {
    return nettyAcceptThreads;
  }

  public void setNettyAcceptThreads(int nettyAcceptThreads) {
    this.nettyAcceptThreads = nettyAcceptThreads;
  }

  public int getNettyWorkerThreads() {
    return nettyWorkerThreads;
  }

  public void setNettyWorkerThreads(int nettyWorkerThreads) {
    this.nettyWorkerThreads = nettyWorkerThreads;
  }

  public int getNetworkBacklog() {
    return networkBacklog;
  }

  public void setNetworkBacklog(int networkBacklog) {
    this.networkBacklog = networkBacklog;
  }

  public int getNetworkTimeout() {
    return networkTimeout;
  }

  public void setNetworkTimeout(int networkTimeout) {
    this.networkTimeout = networkTimeout;
  }

  public int getNetworkRetries() {
    return networkRetries;
  }

  public void setNetworkRetries(int networkRetries) {
    this.networkRetries = networkRetries;
  }

  public ShuffleStorage getStorage() {
    return storage;
  }

  public void setStorage(ShuffleStorage storage) {
    this.storage = storage;
  }

  public boolean isFsyncEnabled() {
    return isFsyncEnabled;
  }

  public void setFsyncEnabled(boolean isFsyncEnabled) {
    this.isFsyncEnabled = isFsyncEnabled;
  }

  public boolean isJFxDebugProfilerEnable() {
    return isJFxDebugProfilerEnable;
  }

  public void setJFxDebugProfilerEnable(boolean isJFxDebugProfilerEnable) {
    this.isJFxDebugProfilerEnable = isJFxDebugProfilerEnable;
  }

  public int getThrottleMemoryPercentage() {
    return throttleMemoryPercentage;
  }

  public void setThrottleMemoryPercentage(int throttleMemoryPercentage) {
    this.throttleMemoryPercentage = throttleMemoryPercentage;
  }

  public long getMaxUploadPauseMillis() {
    return maxUploadPauseMillis;
  }

  public void setMaxUploadPauseMillis(long maxUploadPauseMillis) {
    this.maxUploadPauseMillis = maxUploadPauseMillis;
  }

  public boolean isDaemonExecutorThread() {
    return isDaemonExecutorThread;
  }

  public void setDaemonExecutorThread(boolean isDaemonExecutorThread) {
    this.isDaemonExecutorThread = isDaemonExecutorThread;
  }

  public int getNumSplits() {
    return numSplits;
  }

  public void setNumSplits(int numSplits) {
    this.numSplits = numSplits;
  }

  public long getAppMemoryRetentionMillis() {
    return appMemoryRetentionMillis;
  }

  public void setAppMemoryRetentionMillis(long appMemoryRetentionMillis) {
    this.appMemoryRetentionMillis = appMemoryRetentionMillis;
  }

  public long getAppFileRetentionMillis() {
    return appFileRetentionMillis;
  }

  public void setAppFileRetentionMillis(long appFileRetentionMillis) {
    this.appFileRetentionMillis = appFileRetentionMillis;
  }

  public String getServiceRegistryType() {
    return serviceRegistryType;
  }

  public void setServiceRegistryType(String serviceRegistryType) {
    this.serviceRegistryType = serviceRegistryType;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public String getDataCenterOrDefault() {
    return StringUtils.isBlank(dataCenter) ? DEFAULT_DATA_CENTER : dataCenter;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getClusterOrDefault() {
    return StringUtils.isBlank(cluster) ? ServiceRegistry.DEFAULT_TEST_CLUSTER : cluster;
  }

  public String getZooKeeperServers() {
    return zooKeeperServers;
  }

  public void setZooKeeperServers(String zooKeeperServers) {
    this.zooKeeperServers = zooKeeperServers;
  }

  public String getZooKeeperServersBackup() {
    return zooKeeperServersBackup;
  }

  public void setZooKeeperServersBackup(String value) {
    this.zooKeeperServersBackup = zooKeeperServersBackup;
  }

  public String getRegistryServer() {
    return registryServer;
  }

  public void setRegistryServer(String registryServer) {
    this.registryServer = registryServer;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public String getNetworkCompressionCodec() {
    return networkCompressionCodec;
  }

  public void setNetworkCompressionCodec(String networkCompressionCodec) {
    this.networkCompressionCodec = networkCompressionCodec;
  }

  public String getFileCompressionCodec() {
    return fileCompressionCodec;
  }

  public void setFileCompressionCodec(String fileCompressionCodec) {
    this.fileCompressionCodec = fileCompressionCodec;
  }

  public long getIdleTimeoutMillis() {
    return idleTimeoutMillis;
  }

  public void setIdleTimeoutMillis(long idleTimeoutMillis) {
    this.idleTimeoutMillis = idleTimeoutMillis;
  }

  public long getAppMaxWriteBytes() {
    return appMaxWriteBytes;
  }

  public void setAppMaxWriteBytes(long appMaxWriteBytes) {
    this.appMaxWriteBytes = appMaxWriteBytes;
  }

  public long getStateCommitIntervalMillis() {
    return stateCommitIntervalMillis;
  }

  public void setStateCommitIntervalMillis(long stateCommitIntervalMillis) {
    this.stateCommitIntervalMillis = stateCommitIntervalMillis;
  }

  @Override
  public String toString() {
    return "StreamServerConfig{" +
        "useEpoll=" + useEpoll +
        ", shufflePort=" + shufflePort +
        ", httpPort=" + httpPort +
        ", rootDir='" + rootDir + '\'' +
        ", nettyAcceptThreads=" + nettyAcceptThreads +
        ", nettyWorkerThreads=" + nettyWorkerThreads +
        ", networkBacklog=" + networkBacklog +
        ", networkTimeout=" + networkTimeout +
        ", storage=" + storage +
        ", isFsyncEnabled=" + isFsyncEnabled +
        ", throttleMemoryPercentage=" + throttleMemoryPercentage +
        ", maxUploadPauseMillis=" + maxUploadPauseMillis +
        ", isDaemonExecutorThread=" + isDaemonExecutorThread +
        ", numSplits=" + numSplits +
        ", appMemoryRetentionMillis=" + appMemoryRetentionMillis +
        ", appFileRetentionMillis=" + appFileRetentionMillis +
        ", appMaxWriteBytes=" + appMaxWriteBytes +
        ", serviceRegistry='" + serviceRegistryType + '\'' +
        ", dataCenter='" + dataCenter + '\'' +
        ", cluster='" + cluster + '\'' +
        ", zooKeeperServers='" + zooKeeperServers + '\'' +
        ", maxConnections=" + maxConnections +
        ", networkCompressionCodec=" + networkCompressionCodec +
        ", fileCompressionCodec=" + fileCompressionCodec +
        ", idleTimeoutMillis=" + idleTimeoutMillis +
        ", keytab='" + keytab + '\'' +
        ", principal='" + principal + '\'' +
        ", hadoopConfig='" + hadoopConfig + '\'' +
        ", stateCommitIntervalMillis='" + stateCommitIntervalMillis + '\'' +
        ", isJFxDebugProfilerEnable='" + isJFxDebugProfilerEnable + '\'' +
        '}';
  }
}
