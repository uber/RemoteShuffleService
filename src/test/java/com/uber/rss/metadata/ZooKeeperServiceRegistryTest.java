package com.uber.rss.metadata;

import com.uber.rss.exceptions.RssException;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.ZooKeeperTestCluster;
import com.uber.rss.util.NetworkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ZooKeeperServiceRegistryTest {
    private ZooKeeperTestCluster zooKeeperTestCluster = null;
    private ZooKeeperServiceRegistry serviceRegistry = null;
    private CuratorFramework zk = null;

    @BeforeMethod
    public void setUp() {
        zooKeeperTestCluster = new ZooKeeperTestCluster();
        serviceRegistry = new ZooKeeperServiceRegistry(zooKeeperTestCluster.getZooKeeperServers(), ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS, ServiceRegistryTestUtil.ZK_MAX_RETRIES);
        zk = serviceRegistry.getZooKeeper();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        serviceRegistry.close();
        zooKeeperTestCluster.stop();
    }

    private List<String> getChildren(String path, boolean mustExist) {
        try {
            return zk.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException nne) {
            if (mustExist) Assert.fail("Unable to get children for "+path, nne);
            return Collections.emptyList();
        } catch (Exception e) {
             Assert.fail("Unable to get children for "+path, e);
        }
        return null;
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_NullDataCenter() {
        serviceRegistry.registerServer(null, "cluster1", "server1", "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_EmptyDataCenter() {
        serviceRegistry.registerServer("", "cluster1", "server1", "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_NullCluster() {
        serviceRegistry.registerServer("dc1", null, "server1", "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_EmptyCluster() {
        serviceRegistry.registerServer("dc1", "", "server1", "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_NullServerId() {
        serviceRegistry.registerServer("dc1", "cluster1", null, "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_EmptyServerId() {
        serviceRegistry.registerServer("dc1", "cluster1", "", "v1", "node1:1");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_NullHostPort() {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", null);
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void registerServer_EmptyHostPort() {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "");
    }

    @Test
    public void testRegisterServer() throws Exception {
        List<String> children = null;

        String nodesPath = "/spark_rss/dc1/cluster1/nodes";

        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "abcd:1234");
        children = getChildren(nodesPath, true);
        Assert.assertNotNull(children);
        Assert.assertEquals(children, Collections.singletonList("server1"));

        serviceRegistry.registerServer("dc1", "cluster1","server2", "v1", "efgh:5678");
        children = getChildren(nodesPath, true);
        Assert.assertNotNull(children);
        Collections.sort(children);
        Assert.assertEquals(children, Arrays.asList("server1", "server2"));

        // Add the same node a second time, it won't affect the result
        serviceRegistry.registerServer("dc1", "cluster1","server1", "v1", "efgh:5678");
        children = getChildren(nodesPath, true);
        Assert.assertNotNull(children);
        Collections.sort(children);
        Assert.assertEquals(children, Arrays.asList("server1", "server2"));

        // Nodes are ephemeral, should be gone once a connection dies.
        // Check that it's there with a new connection, and gone eventually once the original is killed.
        try (ZooKeeperServiceRegistry store2 = new ZooKeeperServiceRegistry(zooKeeperTestCluster.getZooKeeperServers(), ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS, ServiceRegistryTestUtil.ZK_MAX_RETRIES)) {
            try {
                children = store2.getZooKeeper().getChildren().forPath("/spark_rss/dc1/cluster1/nodes");
            } catch (Exception e) {
                Assert.fail("Unable to get children for " + nodesPath, e);
            }
            Collections.sort(children);
            Assert.assertEquals(children, Arrays.asList("server1", "server2"));
            zk.close();

            long stepMillis = 500;
            for (int t = 0; t < 10000 && (children == null || children.size() > 0); t += stepMillis) {
                Thread.sleep(stepMillis);
                try {
                    children = store2.getZooKeeper().getChildren().forPath(nodesPath);
                } catch (Exception e) {
                    Assert.fail("Unable to get children for " + nodesPath, e);
                }
            }
            Assert.assertEquals(children.size(), 0, "Nodes should have been deleted, but were not.");
        }
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
    }

    @Test
    public void testRegisterLocalServer() {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", String.format("%s:%s", NetworkUtils.getLocalFQDN(), 123));
        List<ServerDetail> nodes = serviceRegistry.getServers("dc1", "cluster1", Integer.MAX_VALUE, Collections.emptyList());
        Assert.assertEquals(nodes.size(), 1);
        Assert.assertEquals(nodes.get(0).getServerId(), "server1");
        Assert.assertEquals(nodes.get(0).getConnectionString(), NetworkUtils.getLocalFQDN() + ":" + 123);
    }

    @Test
    public void testClose() {
        ZooKeeperServiceRegistry store2 = new ZooKeeperServiceRegistry(zooKeeperTestCluster.getZooKeeperServers(), ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS, ServiceRegistryTestUtil.ZK_MAX_RETRIES);
        CuratorFramework zk2 = store2.getZooKeeper();
        CuratorFrameworkState state = zk2.getState();
        Assert.assertEquals(state, CuratorFrameworkState.STARTED);
        store2.close();
        Assert.assertNull(store2.getZooKeeper());
        state = zk2.getState();
        Assert.assertEquals(state, CuratorFrameworkState.STOPPED);
    }

    @Test
    public void testCreateTimingInstance() throws Exception {
        ServiceRegistry instance = ZooKeeperServiceRegistry.createTimingInstance(zooKeeperTestCluster.getZooKeeperServers(), ServiceRegistryTestUtil.ZK_TIMEOUT_MILLIS, ServiceRegistryTestUtil.ZK_MAX_RETRIES);
        instance.close();
        instance.close();
    }

    @Test
    public void lookupServers_emptyList() {
        ServiceRegistryTestUtil.lookupServers_emptyList(serviceRegistry);
    }

    @Test(expectedExceptions = {RssException.class})
    public void lookupServers_dcWithoutServerRegistered() {
        ServiceRegistryTestUtil.lookupServers_dcWithoutServerRegistered(serviceRegistry);
    }

    @Test(expectedExceptions = {RssException.class})
    public void lookupServers_clusterWithoutServerRegistered() {
        ServiceRegistryTestUtil.lookupServers_clusterWithoutServerRegistered(serviceRegistry);
    }

    @Test(expectedExceptions = {RssException.class})
    public void lookupServers_badServerIds() {
        ServiceRegistryTestUtil.lookupServers_badServerIds(serviceRegistry);
    }

    @Test(expectedExceptions = {RssException.class})
    public void lookupServers_oneGoodServerIdOneBad() {
        ServiceRegistryTestUtil.lookupServers_oneGoodServerIdOneBad(serviceRegistry);
    }
}
