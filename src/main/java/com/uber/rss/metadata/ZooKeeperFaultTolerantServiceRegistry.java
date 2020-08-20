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

import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.common.ServerDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This service registry supports two lists of zookeeper servers, and will try both of them for fault tolerant.
 * This could be used when we switch zookeeper servers to avoid downtime.
 */
public class ZooKeeperFaultTolerantServiceRegistry implements ServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperFaultTolerantServiceRegistry.class);

    private final List<String> zkServers;
    private final List<ZooKeeperServiceRegistry> zooKeeperServiceRegistries;

    public static ServiceRegistry createTimingInstance(Collection<String> zkServers, int timeoutMillis, int maxRetries) {
        return new ServiceRegistryWrapper(
            new ZooKeeperFaultTolerantServiceRegistry(zkServers, timeoutMillis, maxRetries));
    }

    public ZooKeeperFaultTolerantServiceRegistry(Collection<String> zkServers, int timeoutMillis, int maxRetries) {
        this.zkServers = new ArrayList<>(zkServers);
        this.zooKeeperServiceRegistries = zkServers.stream().map(t->new ZooKeeperServiceRegistry(t, timeoutMillis, maxRetries)).collect(Collectors.toList());
    }

    @Override
    public void registerServer(String dataCenter, String cluster, String serverId, String runningVersion, String hostAndPort) {
        zooKeeperServiceRegistries.stream().forEach(t->t.registerServer(dataCenter, cluster, serverId, runningVersion, hostAndPort));
    }

    @Override
    public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount, Collection<String> excludeHosts) {
        return invokeUnderlyingRegistries(registry -> registry.getServers(dataCenter, cluster, maxCount, excludeHosts));
    }

    @Override
    public List<ServerDetail> lookupServers(String dataCenter, String cluster, Collection<String> serverIds) {
        return invokeUnderlyingRegistries(registry -> registry.lookupServers(dataCenter, cluster, serverIds));
    }

    @Override
    public void close() {
        for (ZooKeeperServiceRegistry registry: zooKeeperServiceRegistries) {
            try {
                logger.info("Closing " + registry);
                registry.close();
            } catch (Throwable ex) {
                logger.warn("Failed to close " + registry, ex);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("ZooKeeperFaultTolerantServiceRegistry{servers=%s}", String.join(" | ", zkServers));
    }

    private <T> T invokeUnderlyingRegistries(Function<ZooKeeperServiceRegistry, T> func) {
        List<Throwable> exceptions = new ArrayList<>();

        for (ZooKeeperServiceRegistry registry: zooKeeperServiceRegistries) {
            try {
                logger.info("Trying " + registry);
                return func.apply(registry);
            } catch (Throwable ex) {
                exceptions.add(ex);
            }
        }

        throw new RssAggregateException(exceptions);
    }
}
