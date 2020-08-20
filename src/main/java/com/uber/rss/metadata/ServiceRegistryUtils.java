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

package com.uber.rss.metadata;

import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.ScopeCloseException;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssServerDownException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.NetworkUtils;
import com.uber.rss.util.RetryUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class ServiceRegistryUtils {
    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistryUtils.class);

    /***
     * Get all servers from service registry with retry
     * @param serviceRegistry service registry instance
     * @param maxServerCount max server count to return
     * @param maxTryMillis max trying milliseconds
     * @param dataCenter data center
     * @param cluster cluster
     * @return servers
     */
    public static List<ServerDetail> getReachableServers(ServiceRegistry serviceRegistry, int maxServerCount, long maxTryMillis, String dataCenter, String cluster, Collection<String> excludeHosts) {
        int retryIntervalMillis = 100;
        List<ServerDetail> serverInfos = RetryUtils.retryUntilNotNull(
                retryIntervalMillis,
                maxTryMillis,
                () -> {
                    try {
                        logger.info(String.format("Trying to get max %s RSS servers, data center: %s, cluster: %s, exclude hosts: %s", maxServerCount, dataCenter, cluster, StringUtils.join(excludeHosts, ",")));
                        return serviceRegistry.getServers(dataCenter, cluster, maxServerCount, excludeHosts);
                    } catch (Throwable ex) {
                        logger.warn("Failed to call ServiceRegistry.getServers", ex);
                        return null;
                    }
                });
        if (serverInfos == null || serverInfos.isEmpty()) {
            throw new RssException("Failed to get all RSS servers");
        }

        // some hosts may get UnknowHostException sometimes, exclude those hosts
        logger.info(String.format("Got %s RSS servers from service registry, checking their connectivity", serverInfos.size()));
        ConcurrentLinkedQueue<String> unreachableHosts = new ConcurrentLinkedQueue<>();
        serverInfos = serverInfos.parallelStream().filter(t -> {
          HostAndPort hostAndPort = HostAndPort.fromString(t.getConnectionString());
          String host = hostAndPort.getHostText();
          boolean reachable =  NetworkUtils.isReachable(host, NetworkUtils.DEFAULT_REACHABLE_TIMEOUT);
          if (!reachable) {
            logger.warn(String.format("Detected unreachable host %s", host));
            unreachableHosts.add(host);
          }
          return reachable;
        }).collect(Collectors.toList());

        for (String unreachableHost : unreachableHosts) {
          Map<String, String> tags = new HashMap<>();
          tags.put("remote", unreachableHost);
          Scope scope = M3Stats.createSubScope(tags);
          scope.counter("unreachableHosts").inc(1);
        }

        return serverInfos;
    }

    /***
     * Look up servers by server ids.
     * @param serviceRegistry service registry
     * @param maxTryMillis max trying milliseconds
     * @param dataCenter data center
     * @param cluster cluster
     * @param serverIds list of server ids
     * @return servers
     */
    public static List<ServerDetail> lookupServers(ServiceRegistry serviceRegistry, long maxTryMillis, String dataCenter, String cluster, Collection<String> serverIds) {
        int retryIntervalMillis = 100;
        return RetryUtils.retryUntilNotNull(
                retryIntervalMillis,
                maxTryMillis,
                () -> {
                    try {
                        logger.info(String.format(
                            "Trying to look up RSS servers (data center: %s, cluster: %s) for %s",
                            dataCenter, cluster, serverIds.stream().collect(Collectors.joining(","))));
                        return serviceRegistry.lookupServers(dataCenter, cluster, serverIds);
                    } catch (Throwable ex) {
                        logger.warn("Failed to call ServiceRegistry.lookupServers", ex);
                        return null;
                    }
                });
    }

  /***
   * Check whether the servers are alive
   * @param serviceRegistry service registry
   * @param dataCenter data center
   * @param cluster cluster
   * @param servers servers to check
   */
    public static void checkServersAlive(ServiceRegistry serviceRegistry, String dataCenter, String cluster, Collection<ServerDetail> servers) {
      List<String> serverIds = servers.stream().map(t->t.getServerId()).collect(Collectors.toList());
      List<ServerDetail> latestServers;
      try {
        latestServers = serviceRegistry.lookupServers(dataCenter, cluster, serverIds);
      } catch (Throwable ex) {
        String serversInfo = servers.stream().map(t->t.toString()).collect(Collectors.joining(", "));
        throw new RssServerDownException(String.format("Some of the servers were down: %s", serversInfo));
      }

      List<ServerDetail> oldServers = new ArrayList<>(servers);
      for (int i = 0; i < servers.size(); i++) {
        ServerDetail oldServer = oldServers.get(i);
        ServerDetail latestServer = latestServers.get(i);
        if (!oldServer.equals(latestServer)) {
          throw new RssServerDownException(String.format("Server was restarted: %s", oldServer));
        }
      }
    }

    public static List<ServerDetail>  excludeByHosts(List<ServerDetail> servers, int maxCount, Collection<String> excludeHosts) {
        return servers.stream().filter(t->!shouldExclude(t.getConnectionString(), excludeHosts))
          .limit(maxCount)
          .collect(Collectors.toList());
    }

    private static boolean shouldExclude(String connectionString, Collection<String> excludeHosts) {
      for (String str: excludeHosts) {
        if (connectionString.contains(str)) {
          return true;
        }
      }
      return false;
    }
}
