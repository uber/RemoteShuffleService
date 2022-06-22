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

import com.uber.rss.common.ServerDetail;
import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class RegistryClientTest {

  @Test
  public void clientConnectToServer() {
    TestStreamServer testServer = TestStreamServer.createRunningServer();

    try (RegistryClient client = new RegistryClient("localhost", testServer.getShufflePort(),
        TestConstants.NETWORK_TIMEOUT, "user1")) {
      client.connect();

      List<ServerDetail> servers = client.getServers("dc1", "cluster1", 0);
      Assert.assertEquals(servers.size(), 0);

      servers = client.getServers("dc1", "cluster1", 10);
      Assert.assertEquals(servers.size(), 0);

      client.registerServer("dc1", "cluster1", "server1", "host1:1");
      servers = client.getServers("dc1", "cluster1", 10);
      Assert.assertEquals(servers.size(), 1);
      Assert.assertEquals(servers, Arrays.asList(new ServerDetail("server1", "host1:1")));

      servers = client.getServers("dc1", "cluster2", 10);
      Assert.assertEquals(servers.size(), 0);
    } finally {
      testServer.shutdown();
    }
  }

}
