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

package com.uber.rss.clients;

import com.uber.rss.clients.PooledRecordSyncWriteClient;
import com.uber.rss.clients.PooledWriteClientFactory;
import com.uber.rss.clients.RecordSyncWriteClient;
import com.uber.rss.exceptions.RssAggregateException;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

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

      ConcurrentLinkedQueue<Throwable> threadExceptions = new ConcurrentLinkedQueue<>();

      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
        Thread thread = new Thread(() -> {
          int numIterationsInsideThread = 1000;
          for (int iterationsInsideThread = 0; iterationsInsideThread < numIterationsInsideThread; iterationsInsideThread++) {
            try (RecordSyncWriteClient writeClient = writeClientFactory.getOrCreateClient("localhost", testServer1.getShufflePort(), TestConstants.NETWORK_TIMEOUT, true, "user1", appId, appAttempt, TestConstants.SHUFFLE_WRITE_CONFIG)) {
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
