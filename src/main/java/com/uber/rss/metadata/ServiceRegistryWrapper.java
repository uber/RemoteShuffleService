/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.metadata;

import com.uber.m3.tally.Stopwatch;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssInvalidStateException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.MetadataClientMetrics;
import com.uber.rss.metrics.MetadataClientMetricsContainer;
import com.uber.rss.util.ExceptionUtils;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * This class wraps a ServiceRegistry instance and add metrics for its method call.
 */
public class ServiceRegistryWrapper implements ServiceRegistry {
  private static MetadataClientMetricsContainer metricsContainer =
      new MetadataClientMetricsContainer();

  private ServiceRegistry delegate;

  public ServiceRegistryWrapper(ServiceRegistry delegate) {
    this.delegate = delegate;
  }

  @Override
  public void registerServer(String dataCenter, String cluster, String serverId,
                             String hostAndPort) {
    invokeRunnable(
        () -> delegate.registerServer(dataCenter, cluster, serverId, hostAndPort),
        "registerServer");
  }

  @Override
  public List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount,
                                       Collection<String> excludeHosts) {
    return invokeRunnable(
        () -> delegate.getServers(dataCenter, cluster, maxCount, excludeHosts),
        "getServers");
  }

  @Override
  public List<ServerDetail> lookupServers(String dataCenter, String cluster,
                                          Collection<String> serverIds) {
    return invokeRunnable(
        () -> delegate.lookupServers(dataCenter, cluster, serverIds),
        "lookupServers");
  }

  @Override
  public void close() {
    invokeRunnable(
        () -> delegate.close(),
        "close");
  }

  private void invokeRunnable(Runnable runnable, String name) {
    String className = delegate.getClass().getSimpleName();
    MetadataClientMetrics metrics = getMetrics(className, name);
    Stopwatch stopWatch = metrics.getRequestLatency().start();
    try {
      runnable.run();
    } catch (Throwable e) {
      metrics.getNumFailures().inc(1);
      M3Stats.addException(e, this.getClass().getSimpleName());
      ExceptionUtils.throwException(e);
    } finally {
      stopWatch.stop();
    }
  }

  private <T> T invokeRunnable(Supplier<T> runnable, String name) {
    String className = delegate.getClass().getSimpleName();
    MetadataClientMetrics metrics = getMetrics(className, name);
    Stopwatch stopWatch = metrics.getRequestLatency().start();
    try {
      return runnable.get();
    } catch (Throwable e) {
      metrics.getNumFailures().inc(1);
      M3Stats.addException(e, this.getClass().getSimpleName());
      ExceptionUtils.throwException(e);
      throw new RssInvalidStateException(
          "Should not run into here because the previous line of code will throw out exception");
    } finally {
      stopWatch.stop();
    }
  }

  private MetadataClientMetrics getMetrics(String className, String operation) {
    MetadataClientMetrics metrics = metricsContainer.getMetricGroup(className, operation);
    metrics.getNumRequests().inc(1);
    return metrics;
  }
}
