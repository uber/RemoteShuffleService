package com.uber.rss.metadata;

import com.uber.rss.clients.RegistryClient;
import com.uber.rss.common.ServerDetail;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/***
 * This is a service registry client connecting to a standalone registry server, which could be used in
 * a test environment without zookeeper, e.g. open source.
 * TODO this class currently create a new connection for each method call. Will improve that in future
 * to reuse connection and also recreate connection on failures.
 */
public class StandaloneServiceRegistryClient implements ServiceRegistry {
  private final String host;
  private final int port;
  private final int timeoutMillis;
  private final String user;

  public StandaloneServiceRegistryClient(String host, int port, int timeoutMillis, String user) {
    this.host = host;
    this.port = port;
    this.timeoutMillis = timeoutMillis;
    this.user = user;
  }

  @Override
  public void registerServer(String dataCenter, String cluster, String serverId, String runningVersion, String hostAndPort) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      registryClient.registerServer(dataCenter, cluster, serverId, runningVersion, hostAndPort);
    }
  }

  @Override
  public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount, Collection<String> excludeHosts) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      List<ServerDetail> list = registryClient.getServers(dataCenter, cluster, maxCount + excludeHosts.size());
      return ServiceRegistryUtils.excludeByHosts(list, maxCount, excludeHosts);
    }
  }

  @Override
  public List<ServerDetail> lookupServers(String dataCenter, String cluster, Collection<String> serverIds) {
    try (RegistryClient registryClient = new RegistryClient(host, port, timeoutMillis, user)) {
      registryClient.connect();
      List<ServerDetail> servers = registryClient.getServers(dataCenter, cluster, Integer.MAX_VALUE);
      return servers.stream().filter(t->serverIds.contains(t.getServerId())).collect(Collectors.toList());
    }
  }

  @Override
  public void close() {
  }
}
