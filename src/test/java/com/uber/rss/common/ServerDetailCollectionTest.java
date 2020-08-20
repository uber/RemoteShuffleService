package com.uber.rss.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ServerDetailCollectionTest {
    @Test
    public void addServer() {
        ServerDetailCollection serverDetailCollection = new ServerDetailCollection();

        Assert.assertEquals(serverDetailCollection.getServers("dc1", "cluster1").size(), 0);

        serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server1", "v1", "node1:1"));
        serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server1", "v1", "node2:2"));

        Assert.assertEquals(serverDetailCollection.getServers("dc1", "cluster1"),
                Arrays.asList(new ServerDetail("server1", "v1", "node2:2")));

        serverDetailCollection.addServer("dc1", "cluster1", new ServerDetail("server2", "v1", "node2:2"));

        List<ServerDetail> servers = serverDetailCollection.getServers("dc1", "cluster1");
        servers.sort(Comparator.comparing(ServerDetail::getServerId));
        Assert.assertEquals(servers,
                Arrays.asList(new ServerDetail("server1", "v1", "node2:2"), new ServerDetail("server2", "v1", "node2:2")));
    }
}
