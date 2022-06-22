/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss.clients;

import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.annotations.Test;

public class HeartbeatSocketClientTest {
  @Test
  public void createInstance() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    boolean keepLive = false;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
      client.close();
      client.close();
    }

    keepLive = true;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
      client.close();
      client.close();
    }

    testServer1.shutdown();
  }

  @Test
  public void sendHeartbeat_keepLiveFalse() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    boolean keepLive = false;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
      client.sendHeartbeat();
      client.close();
    }

    testServer1.shutdown();
  }

  @Test
  public void sendHeartbeat_keepLiveTrue() {
    TestStreamServer testServer1 = TestStreamServer.createRunningServer();

    boolean keepLive = true;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
      client.sendHeartbeat();
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1",
        "appAttempt1", keepLive)) {
      client.sendHeartbeat();
      client.sendHeartbeat();
      client.close();
    }

    testServer1.shutdown();
  }
}
