package com.uber.rss.metadata;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StandalongServiceRegistryClientTest {

    @Test
    public void getServers() {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try (StandaloneServiceRegistryClient client = new StandaloneServiceRegistryClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
            List<ServerDetail> servers = client.getServers("dc1", "cluster1", 0, Collections.emptyList());
            Assert.assertEquals(servers.size(), 0);

            servers = client.getServers("dc1", "cluster1", 10, Collections.emptyList());
            Assert.assertEquals(servers.size(), 0);

            client.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
            servers = client.getServers("dc1", "cluster1", 10, Collections.emptyList());
            Assert.assertEquals(servers.size(), 1);
            Assert.assertEquals(servers, Arrays.asList(new ServerDetail("server1", "v1", "host1:1")));

            servers = client.getServers("dc1", "cluster2", 10, Collections.emptyList());
            Assert.assertEquals(servers.size(), 0);

            client.registerServer("dc1", "cluster1", "server2", "v2", "host2:2");
            client.registerServer("dc1", "cluster1", "server3", "v3", "host3:3");

            servers = client.lookupServers("dc1", "cluster1", Arrays.asList("server2", "server3"));
            Assert.assertEquals(servers.size(), 2);
            servers.sort(Comparator.comparing(ServerDetail::getServerId));
            Assert.assertEquals(servers,
                Arrays.asList(
                    new ServerDetail("server2", "v2", "host2:2"),
                    new ServerDetail("server3", "v3", "host3:3")));
        } finally {
            testServer.shutdown();
        }
    }

}
