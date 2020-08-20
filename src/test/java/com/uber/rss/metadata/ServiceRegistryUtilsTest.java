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

import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssServerDownException;
import com.uber.rss.testutil.ZooKeeperTestCluster;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

public class ServiceRegistryUtilsTest {
  private ZooKeeperTestCluster zooKeeperTestCluster = null;
  private ZooKeeperServiceRegistry serviceRegistry = null;

  @BeforeMethod
  public void setUp() {
    zooKeeperTestCluster = new ZooKeeperTestCluster();
    serviceRegistry = new ZooKeeperServiceRegistry(zooKeeperTestCluster.getZooKeeperServers(), ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS, ServiceRegistryTestUtil.ZK_MAX_RETRIES);
  }

  @AfterMethod
  public void tearDown() {
    serviceRegistry.close();
    zooKeeperTestCluster.stop();
  }

  @Test
  public void checkServersAlive() {
    String dataCenter = "dc1";
    String cluster = "cluster1";
    serviceRegistry.registerServer(dataCenter, cluster, "server1", "12345", "server:9990");
    serviceRegistry.registerServer(dataCenter, cluster, "server2", "12346", "server:9991");
    serviceRegistry.registerServer(dataCenter, cluster, "server3", "12347", "server:9992");
    ServiceRegistryUtils.checkServersAlive(serviceRegistry,
        dataCenter,
        cluster,
        Arrays.asList(new ServerDetail("server2", "12346", "server:9991"), new ServerDetail("server1", "12345", "server:9990")));
  }

  @Test(expectedExceptions = {RssServerDownException.class})
  public void checkServersAlive_noRegisteredServer() {
    String dataCenter = "dc1";
    String cluster = "cluster1";
    ServiceRegistryUtils.checkServersAlive(serviceRegistry,
        dataCenter,
        cluster,
        Arrays.asList(new ServerDetail("server1", "12345", "server:9999")));
  }

  @Test(expectedExceptions = {RssServerDownException.class})
  public void checkServersAlive_changedServerVersion() {
    String dataCenter = "dc1";
    String cluster = "cluster1";
    serviceRegistry.registerServer(dataCenter, cluster, "server1", "12345", "server:9990");
    serviceRegistry.registerServer(dataCenter, cluster, "server2", "12346", "server:9991");
    serviceRegistry.registerServer(dataCenter, cluster, "server3", "12347", "server:9992");
    ServiceRegistryUtils.checkServersAlive(serviceRegistry,
        dataCenter,
        cluster,
        Arrays.asList(new ServerDetail("server2", "12346", "server:9991"), new ServerDetail("server1", "12347", "server:9990")));
  }

  @Test(expectedExceptions = {RssServerDownException.class})
  public void checkServersAlive_changedServerConnectionString() {
    String dataCenter = "dc1";
    String cluster = "cluster1";
    serviceRegistry.registerServer(dataCenter, cluster, "server1", "12345", "server:9990");
    serviceRegistry.registerServer(dataCenter, cluster, "server2", "12346", "server:9991");
    serviceRegistry.registerServer(dataCenter, cluster, "server3", "12347", "server:9992");
    ServiceRegistryUtils.checkServersAlive(serviceRegistry,
        dataCenter,
        cluster,
        Arrays.asList(new ServerDetail("server2", "12346", "server:9991"), new ServerDetail("server1", "12345", "server:9992")));
  }
}
