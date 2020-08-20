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
import com.uber.rss.testutil.ZooKeeperTestCluster;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ZooKeeperFaultTolerantServiceRegistryTest {
    private ZooKeeperTestCluster zooKeeperTestCluster1 = null;
    private ZooKeeperTestCluster zooKeeperTestCluster2 = null;
    private ZooKeeperFaultTolerantServiceRegistry serviceRegistry = null;

    @BeforeMethod
    public void setUp() {
        zooKeeperTestCluster1 = new ZooKeeperTestCluster();
        zooKeeperTestCluster2 = new ZooKeeperTestCluster();

        serviceRegistry = new ZooKeeperFaultTolerantServiceRegistry(
                Arrays.asList(zooKeeperTestCluster1.getZooKeeperServers(), zooKeeperTestCluster2.getZooKeeperServers()),
                ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS,
                ServiceRegistryTestUtil.ZK_MAX_RETRIES);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        serviceRegistry.close();
        zooKeeperTestCluster1.stop();
        zooKeeperTestCluster2.stop();
    }

    @Test
    public void testGetServers() {
        List<ServerDetail> nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Assert.assertEquals(nodes.size(), 0);

        serviceRegistry.registerServer("dc1", "cluster1", "server1:/root/dir", "v1", "server1:123");

        nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Assert.assertEquals(nodes, Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:123")));

        serviceRegistry.registerServer("dc1", "cluster1", "server1:/root/dir", "v1", "server1:123");

        nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Assert.assertEquals(nodes, Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:123")));

        serviceRegistry.registerServer("dc1", "cluster1", "server1:/root/dir", "v1", "server1:12345");

        Comparator<ServerDetail> comparator = new Comparator<ServerDetail>() {
            @Override
            public int compare(ServerDetail o1, ServerDetail o2) {
                return o1.getServerId().compareTo(o2.getServerId());
            }
        };

        nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Collections.sort(nodes, comparator);
        Assert.assertEquals(nodes,
                Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:12345")));

        serviceRegistry.registerServer("dc1", "cluster1", "server3:/root/dir3", "v1", "Server3:12345");

        nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Collections.sort(nodes, comparator);
        Assert.assertEquals(nodes,
                Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:12345"),
                        new ServerDetail("server3:/root/dir3", "v1", "Server3:12345")));

        nodes = serviceRegistry.getServers("dc1", "cluster1", 1, Collections.emptyList());
        Assert.assertEquals(nodes.size(), 1);

        nodes = serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server1:/root/dir", "server3:/root/dir3"));
        Assert.assertEquals(nodes,
                Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:12345"),
                        new ServerDetail("server3:/root/dir3", "v1", "Server3:12345")));

        // Shutdown first zookeeper cluster, should still be able to get from the second zookeeper cluster
        zooKeeperTestCluster1.stop();

        nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Collections.sort(nodes, comparator);
        Assert.assertEquals(nodes,
                Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:12345"),
                        new ServerDetail("server3:/root/dir3", "v1", "Server3:12345")));

        nodes = serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server1:/root/dir", "server3:/root/dir3"));
        Assert.assertEquals(nodes,
                Arrays.asList(new ServerDetail("server1:/root/dir", "v1", "server1:12345"),
                        new ServerDetail("server3:/root/dir3", "v1", "Server3:12345")));
    }

    @Test
    public void createTimingInstance() throws Exception {
        ServiceRegistry zooKeeperFaultTolerantServiceRegistry = ZooKeeperFaultTolerantServiceRegistry.createTimingInstance(
                Arrays.asList(zooKeeperTestCluster1.getZooKeeperServers(), zooKeeperTestCluster2.getZooKeeperServers()),
                ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS,
                ServiceRegistryTestUtil.ZK_MAX_RETRIES
        );

        zooKeeperFaultTolerantServiceRegistry.close();
    }
}
