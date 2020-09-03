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
        try {
            testingServer = new TestingServer();
            testingServer.start();
            port = testingServer.getPort();
            logger.info(String.format("Started ZooKeeper testing server on port %s", port));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to start ZooKeeper testing server on port %s", port), e);
        }
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
