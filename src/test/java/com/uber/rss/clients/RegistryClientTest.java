package com.uber.rss.clients;

import com.uber.rss.clients.RegistryClient;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class RegistryClientTest {

    @Test
    public void clientConnectToServer() {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try (RegistryClient client = new RegistryClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
            client.connect();

            List<ServerDetail> servers = client.getServers("dc1", "cluster1", 0);
            Assert.assertEquals(servers.size(), 0);

            servers = client.getServers("dc1", "cluster1", 10);
            Assert.assertEquals(servers.size(), 0);

            client.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
            servers = client.getServers("dc1", "cluster1", 10);
            Assert.assertEquals(servers.size(), 1);
            Assert.assertEquals(servers, Arrays.asList(new ServerDetail("server1", "v1", "host1:1")));

            servers = client.getServers("dc1", "cluster2", 10);
            Assert.assertEquals(servers.size(), 0);
        } finally {
            testServer.shutdown();
        }
    }

}
