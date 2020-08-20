package com.uber.rss.testutil;

import com.uber.rss.util.NetworkUtils;
import com.uber.rss.util.RetryUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ZooKeeperTestCluster {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperTestCluster.class);

    private int port;
    private TestingServer testingServer;

    public ZooKeeperTestCluster() {
        int retryPollMillis = 1;
        int retryMaxWaitMillis = 1000;
        RetryUtils.retryUntilTrue(retryPollMillis, retryMaxWaitMillis, () -> {
            port = NetworkUtils.getAvailablePort();
            try {
                testingServer = new TestingServer(port);
                testingServer.start();
                logger.info(String.format("Started ZooKeeper testing server on port %s", port));
            } catch (Exception e) {
                logger.warn(String.format("Failed to start ZooKeeper testing server on port %s", port), e);
                return false;
            }
            return true;
        });
    }

    public String getZooKeeperServers() {
        return String.format("localhost:%s", port);
    }

    public void stop() {
        try {
            testingServer.stop();
            logger.info(String.format("Stopped ZooKeeper testing server on port %s", port));
        } catch (IOException e) {
            logger.warn("Failed to stop ZooKeeper testing server");
        }
    }
}
