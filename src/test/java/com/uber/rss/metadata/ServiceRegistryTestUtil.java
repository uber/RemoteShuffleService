package com.uber.rss.metadata;

import com.uber.rss.common.ServerDetail;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServiceRegistryTestUtil {
    public final static int ZK_TIMEOUT_MILLIS = 1000;
    public final static int ZK_MAX_RETRIES = 0;

    public static void lookupServers_emptyList(ServiceRegistry serviceRegistry) {
        List<ServerDetail> result = serviceRegistry.lookupServers("dc1", "cluster1", new ArrayList<>());
        Assert.assertEquals(result.size(), 0);
    }

    public static void lookupServers_dcWithoutServerRegistered(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
        serviceRegistry.lookupServers("dc2", "cluster1", Arrays.asList("server1"));
    }

    public static void lookupServers_clusterWithoutServerRegistered(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
        serviceRegistry.lookupServers("dc1", "cluster2", Arrays.asList("server1"));
    }

    public static void lookupServers_badServerIds(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
        serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server3"));
    }

    public static void lookupServers_oneGoodServerIdOneBad(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServer("dc1", "cluster1", "server1", "v1", "host1:1");
        serviceRegistry.lookupServers("dc1", "cluster1", Arrays.asList("server1", "server3"));
    }
}
