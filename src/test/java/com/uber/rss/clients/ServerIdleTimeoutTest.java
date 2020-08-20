package com.uber.rss.clients;

import com.uber.rss.clients.PooledWriteClientFactory;
import com.uber.rss.clients.RecordSyncWriteClient;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssFinishUploadException;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServerIdleTimeoutTest {
  @Test
  public void writeAfterServerIdleTimeout() {
    long serverIdleTimeoutMillis = 10;
    TestStreamServer testServer1 = TestStreamServer.createRunningServer(config -> config.setIdleTimeoutMillis(serverIdleTimeoutMillis));
    PooledWriteClientFactory writeClientFactory = new PooledWriteClientFactory(TestConstants.CONNECTION_IDLE_TIMEOUT_MILLIS);

    try {
      String appId = "app01";
      int shuffleId = 1;

      String appAttempt = "attempt1";
      int numMaps = 1;
      int numPartitions = 10;
      int mapId = 2;
      long taskAttemptId = 11;
      AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId(appId, appAttempt, shuffleId, mapId, taskAttemptId);

      boolean finishUploadAck = true;

      try (RecordSyncWriteClient writeClient = writeClientFactory.getOrCreateClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, finishUploadAck, "user1", appId, appAttempt, TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG)) {
        // sleep sometime so the server thinks the connection is idle and timeout
        try {
          Thread.sleep(serverIdleTimeoutMillis * 2);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        writeClient.startUpload(appTaskAttemptId, numMaps, numPartitions);
        writeClient.finishUpload();

        Assert.fail("The previous code should throw exception because the server should close the connection because of idle timeout");
      } catch (Throwable ex) {
        if (!ex.getClass().equals(RssNetworkException.class) && !ex.getClass().equals(RssFinishUploadException.class)) {
          throw ex;
        }
      }

      Assert.assertEquals(writeClientFactory.getNumIdleClients(), 0);
    } finally {
      testServer1.shutdown();
      writeClientFactory.shutdown();
    }
  }
}
