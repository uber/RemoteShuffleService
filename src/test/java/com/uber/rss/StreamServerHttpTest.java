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

package com.uber.rss;

import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StreamServerHttpTest {
  private static final Logger logger = LoggerFactory.getLogger(StreamServerHttpTest.class);

  @Test
  public void health() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      String url = String.format("http://localhost:%s/health", testServer.getHttpPort());
      String response = HttpUtils.getUrl(url);
      Assert.assertEquals(response, "OK");
    } finally {
      testServer.shutdown();
    }
  }

  @Test
  public void threadDump() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try {
      String url = String.format("http://localhost:%s/threadDump", testServer.getHttpPort());
      String response = HttpUtils.getUrl(url);
      Assert.assertTrue(response.contains("ThreadStackTrace"));
      Assert.assertTrue(response.contains("main"));
    } finally {
      testServer.shutdown();
    }
  }

}
