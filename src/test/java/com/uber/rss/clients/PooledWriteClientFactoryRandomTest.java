package com.uber.rss.clients;

import com.uber.rss.clients.PooledRecordSyncWriteClient;
import com.uber.rss.clients.PooledWriteClientFactory;
import com.uber.rss.clients.RecordSyncWriteClient;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.spark_project.jetty.util.ConcurrentArrayQueue;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PooledWriteClientFactoryRandomTest {

  @Test
  public void writeAndReadRecords() {
    PooledWriteClientFactory writeClientFactory = new PooledWriteClientFactory(TestConstants.CONNECTION_IDLE_TIMEOUT_MILLIS);

    TestStreamServer testServer1 = TestStreamServer.createRunningServer();
    TestStreamServer testServer2 = TestStreamServer.createRunningServer();

    int numThreads = 10;

    try {
      String appId = "app1";
      String appAttempt = "attempt1";

      ConcurrentArrayQueue<Throwable> threadExceptions = new ConcurrentArrayQueue<>();

      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        Thread thread = new Thread(() -> {
          int numIterationsInsideThread = 1000;
          for (int iterationsInsideThread = 0; iterationsInsideThread < numIterationsInsideThread; iterationsInsideThread++) {
            try (RecordSyncWriteClient writeClient = writeClientFactory.getOrCreateClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", appId, appAttempt, TestConstants.COMPRESSION_BUFFER_SIZE, TestConstants.SHUFFLE_WRITE_CONFIG)) {
              PooledRecordSyncWriteClient pooledRecordSyncWriteClient = (PooledRecordSyncWriteClient) writeClient;
              Assert.assertTrue(pooledRecordSyncWriteClient.isReusable());
              writeClient.close();
              Assert.assertTrue(pooledRecordSyncWriteClient.isReusable());
            }
          }
        });
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable throwable) {
            threadExceptions.add(throwable);
          }
        });
        threads[i] = thread;
      }

      for (int i = 0; i < numThreads; i++) {
        threads[i].start();
      }

      for (int i = 0; i < numThreads; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread interrupted", e);
        }
      }

      if (!threadExceptions.isEmpty()) {
        throw new RssAggregateException(threadExceptions);
      }
    } finally {
      testServer1.shutdown();
      testServer2.shutdown();
    }

    Assert.assertTrue(writeClientFactory.getNumCreatedClients() <= numThreads);
    Assert.assertTrue(writeClientFactory.getNumIdleClients() <= numThreads);

    writeClientFactory.shutdown();
  }

}
