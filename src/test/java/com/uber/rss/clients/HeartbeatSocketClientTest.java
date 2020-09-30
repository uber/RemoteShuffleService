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
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
      client.close();
      client.close();
    }

    keepLive = true;
    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
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
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
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
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
      client.sendHeartbeat();
    }

    try (HeartbeatSocketClient client = new HeartbeatSocketClient(
        "localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1", "app1", "appAttempt1", keepLive)) {
      client.sendHeartbeat();
      client.sendHeartbeat();
      client.close();
    }

    testServer1.shutdown();
  }
}
