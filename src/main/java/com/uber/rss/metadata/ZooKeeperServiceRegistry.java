package com.uber.rss.metadata;

import com.uber.rss.exceptions.RssException;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/***
 * ZooKeeper based service registry.
 */
public class ZooKeeperServiceRegistry implements ServiceRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperServiceRegistry.class);

    private static final String ZK_BASE_PATH = "spark_rss";
    private static final String ZK_NODE_SUBPATH = "nodes";

    private final String servers;
    private CuratorFramework zk;

    public static ServiceRegistry createTimingInstance(String zkServers, int timeoutMillis, int maxRetries) {
        return new ServiceRegistryWrapper(
            new ZooKeeperServiceRegistry(zkServers, timeoutMillis, maxRetries));
    }

    public static int getDefaultPort() {
        return 2181;
    }

    public static String getDefaultServers() {
        return "localhost:" + getDefaultPort();
    }

    /***
     * Constructor
     */
    public ZooKeeperServiceRegistry(String zkServers, int timeoutMillis, int maxRetries) {
        servers = zkServers;

        // TODO: Get these values via configuration
        final int MIN_RETRY_SLEEP_MS = 1000;
        final int MAX_RETRY_SLEEP_MS = 10000;

        zk = CuratorFrameworkFactory.builder()
                .connectString(zkServers)
                .connectionTimeoutMs(timeoutMillis)
                .sessionTimeoutMs(timeoutMillis)
                .retryPolicy(new ExponentialBackoffRetry(MIN_RETRY_SLEEP_MS, maxRetries, MAX_RETRY_SLEEP_MS))
                .build();
        zk.start();
        logger.info("Created " + this);
    }

    @Override
    public String toString() {
        return String.format("ZooKeeperServiceRegistry{servers=%s}", servers);
    }

    @Override
    // TODO refine code here to handle race condition
    public synchronized void registerServer(String dataCenter, String cluster, String serverId, String runningVersion, String connectionString) {
        if (StringUtils.isBlank(dataCenter)) {
            throw new IllegalArgumentException(String.format("Invalid input: dataCenter=%s", dataCenter));
        }

        if (StringUtils.isBlank(cluster)) {
            throw new IllegalArgumentException(String.format("Invalid input: cluster=%s", cluster));
        }

        if (StringUtils.isBlank(serverId)) {
            throw new IllegalArgumentException(String.format("Invalid input: serverId=%s", serverId));
        }

        if (StringUtils.isBlank(runningVersion)) {
            throw new IllegalArgumentException(String.format("Invalid input: runningVersion=%s", runningVersion));
        }

        if (StringUtils.isBlank(connectionString)) {
            throw new IllegalArgumentException(String.format("Invalid input: connectionString=%s", connectionString));
        }

        final String nodeName = getNodeName(serverId);
        final String nodePath = getNodePath(dataCenter, cluster, nodeName);

        final ZooKeeperServerNodeData nodeData = new ZooKeeperServerNodeData();
        nodeData.setRunningVersion(runningVersion);
        nodeData.setConnectionString(connectionString);
        final byte[] nodeDataBytes = JsonUtils.serialize(nodeData).getBytes(StandardCharsets.UTF_8);
        // Two loops because first loop will only fail in the unlikely situation that the ephemeral node already exists
        // and needs to be deleted before trying again.
        for (int i = 0; i < 2; i++) {
            try {
                zk.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(nodePath, nodeDataBytes);
                return;
            } catch (KeeperException.NodeExistsException nee) {
                if (i == 1) {
                    M3Stats.addException(nee, this.getClass().getSimpleName());
                    // This is only a problem on the second attempt
                    throw new RssException("Unable to register ZooKeeper node " + nodePath, nee);
                }
                logger.info("ZooKeeper node already exists: " + nodePath, nee);
                try {
                    // Need to delete it so that the ephemeral node remains tied to the lifetime of this connection
                    zk.delete().forPath(nodePath);
                } catch (KeeperException.NoNodeException nne) {
                    // It's a race condition, but it's fine provided that *somebody* deleted it. Try again.
                } catch (Exception e) {
                    M3Stats.addException(e, this.getClass().getSimpleName());
                    throw new RssException("Unable to delete existing ZooKeeper node " + nodePath, e);
                }
            } catch (Exception e) {
                M3Stats.addException(e, this.getClass().getSimpleName());
                throw new RssException("Unable to create ZooKeeper node " + nodePath, e);
            }
        }
        throw new RssException("Too many failures trying to register the node");
    }

    @Override
    public synchronized List<ServerDetail> getServers(String dataCenter, String cluster, int maxCount, Collection<String> excludeHosts) {
        if (StringUtils.isBlank(dataCenter)) {
            throw new IllegalArgumentException(String.format("Invalid input: dataCenter=%s", dataCenter));
        }

        if (StringUtils.isBlank(cluster)) {
            throw new IllegalArgumentException(String.format("Invalid input: cluster=%s", cluster));
        }

        int maxCountWithExcludeHosts = maxCount + excludeHosts.size();
        String path = getRootPath(dataCenter, cluster);
        try {
            List<String> nodes = zk.getChildren().forPath(path);
            if (nodes.size() > maxCountWithExcludeHosts) {
                Collections.shuffle(nodes);
                nodes = nodes.subList(0, maxCountWithExcludeHosts);
            }
            List<ServerDetail> list = nodes.stream().map(t->getServerInfo(path, t)).collect(Collectors.toList());
            return ServiceRegistryUtils.excludeByHosts(list, maxCount, excludeHosts);
        } catch (KeeperException.NoNodeException e) {
            logger.info("Path not found: " + path);
            M3Stats.addException(e, this.getClass().getSimpleName());
            return Collections.emptyList();
        } catch (Exception e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            throw new RssException("Unable to retrieve nodes from ZooKeeper", e);
        }
    }

    @Override
    public List<ServerDetail> lookupServers(String dataCenter, String cluster, Collection<String> serverIds) {
        final String rootPath = getRootPath(dataCenter, cluster);
        return serverIds.stream().map(this::getNodeName)
                .map(nodeName -> getServerInfo(rootPath, nodeName))
                .collect(Collectors.toList());
    }

    public CuratorFramework getZooKeeper() {
        return zk;
    }

    @Override
    public synchronized void close() {
        if (zk == null) {
            logger.warn("ZooKeeper node already null when being closed");
            return;
        }

        zk.close();
        zk = null;
    }

    private String getRootPath(String dataCenter, String cluster) {
        return String.format("/%s/%s/%s/%s", ZK_BASE_PATH, dataCenter, cluster, ZK_NODE_SUBPATH);
    }

    private String getNodePath(String dataCenter, String cluster, String address) {
        return String.format("%s/%s", getRootPath(dataCenter, cluster), address);
    }

    private ServerDetail getServerInfo(String path, String node) {
        String nodePath = String.format("%s/%s", path, node);
        byte[] bytes = null;
        try {
            bytes = zk.getData().forPath(nodePath);
        } catch (Exception e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            throw new RssException(String.format("Failed to get node data for zookeeper node: %s", nodePath), e);
        }

        if (bytes == null) {
            throw new RssException(String.format("Got null data for zookeeper node: %s", nodePath));
        }
        String serverId = getServerId(node);
        String str = new String(bytes, StandardCharsets.UTF_8);
        ZooKeeperServerNodeData nodeData = JsonUtils.deserialize(str, ZooKeeperServerNodeData.class);
        return new ServerDetail(serverId, nodeData.getRunningVersion(), nodeData.getConnectionString());
    }

    private String getNodeName(String serverId) {
        try {
            return URLEncoder.encode(serverId, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            throw new RssException("Failed to get node name for " + serverId, e);
        }
    }

    private String getServerId(String nodeName) {
        try {
            return URLDecoder.decode(nodeName, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            throw new RssException("Failed to get server id for " + nodeName, e);
        }
    }
}
