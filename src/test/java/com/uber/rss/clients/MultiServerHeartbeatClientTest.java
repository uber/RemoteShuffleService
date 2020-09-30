package com.uber.rss.clients;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultiServerHeartbeatClientTest {
  long heartbeatIntervalMillis = 1;
  long networkTimeoutMillis = 20;

  @Test
  public void getInstance() {
    MultiServerHeartbeatClient client = MultiServerHeartbeatClient.getInstance();
    Assert.assertNotNull(client);
  }

  @Test
  public void sendHeartbeats() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
    }
    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.sendHeartbeats();
    }
    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.addServer(testServer.getServerDetail());
      client.sendHeartbeats();

      client.setAppContext("user1", "app1","attempt1");
      client.sendHeartbeats();

      client.addServer(testServer2.getServerDetail());
      client.sendHeartbeats();
    }

    testServer.shutdown();
    testServer2.shutdown();
  }

  @Test
  public void sendHeartbeats_invalidServer() {
    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.addServer(new ServerDetail("invalid_not_existing_server", "100", "invalid_not_existing_server:12345"));
      client.sendHeartbeats();

      client.setAppContext("user1", "app1","attempt1");
      client.sendHeartbeats();
    }
  }

  @Test
  public void sendHeartbeats_refreshConnection() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.setAppContext("user1", "app1","attempt1");
      client.setServerConnectionRefresher(new ServerConnectionRefresher() {
        @Override
        public ServerDetail refreshConnection(ServerDetail serverDetail) {
          return testServer.getServerDetail();
        }
      });
      client.addServer(new ServerDetail(testServer.getServerId(), "100", "invalid_not_existing_server:12345"));

      client.sendHeartbeats();

      client.sendHeartbeats();
    }

    testServer.shutdown();
  }

  @Test
  public void sendHeartbeats_refreshConnectionReturningNull() {
    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.setAppContext("user1", "app1","attempt1");
      client.setServerConnectionRefresher(new ServerConnectionRefresher() {
        @Override
        public ServerDetail refreshConnection(ServerDetail serverDetail) {
          return null;
        }
      });
      client.addServer(new ServerDetail("invalid_not_existing_server", "100", "invalid_not_existing_server:12345"));

      client.sendHeartbeats();

      client.sendHeartbeats();
    }
  }

  @Test
  public void sendHeartbeats_refreshConnectionThrowingException() {
    try (MultiServerHeartbeatClient client = new MultiServerHeartbeatClient(heartbeatIntervalMillis, networkTimeoutMillis)) {
      client.setAppContext("user1", "app1","attempt1");
      client.setServerConnectionRefresher(new ServerConnectionRefresher() {
        @Override
        public ServerDetail refreshConnection(ServerDetail serverDetail) {
          throw new RuntimeException("Injected exception for server connection refresher");
        }
      });
      client.addServer(new ServerDetail("invalid_not_existing_server", "100", "invalid_not_existing_server:12345"));

      client.sendHeartbeats();

      client.sendHeartbeats();
    }
  }
}
