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

package com.uber.rss.util;

import com.uber.rss.exceptions.RssInvalidDataException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServerHostAndPortTest {
  @Test
  public void constructor() {
    ServerHostAndPort hostAndPort = new ServerHostAndPort("server1", 123);
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 123);
    Assert.assertEquals(hostAndPort.toString(), "server1:123");
  }

  @Test
  public void parseString() {
    ServerHostAndPort hostAndPort = new ServerHostAndPort(null);
    Assert.assertEquals(hostAndPort.getHost(), null);
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "null:0");

    hostAndPort = new ServerHostAndPort("");
    Assert.assertEquals(hostAndPort.getHost(), "");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), ":0");

    hostAndPort = new ServerHostAndPort(" ");
    Assert.assertEquals(hostAndPort.getHost(), " ");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), " :0");

    hostAndPort = new ServerHostAndPort("server1");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "server1:0");

    hostAndPort = new ServerHostAndPort("server1:");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 0);
    Assert.assertEquals(hostAndPort.toString(), "server1:0");

    hostAndPort = new ServerHostAndPort("server1:123");
    Assert.assertEquals(hostAndPort.getHost(), "server1");
    Assert.assertEquals(hostAndPort.getPort(), 123);
    Assert.assertEquals(hostAndPort.toString(), "server1:123");
  }

  @Test(expectedExceptions = RssInvalidDataException.class)
  public void invalidPort() {
    new ServerHostAndPort("server1:a");
  }

  @Test(expectedExceptions = RssInvalidDataException.class)
  public void multiPortValues() {
    new ServerHostAndPort("server1:a:b");
  }
}
