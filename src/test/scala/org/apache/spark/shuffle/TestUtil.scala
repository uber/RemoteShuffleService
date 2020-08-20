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

package org.apache.spark.shuffle

import com.uber.rss.metadata.ServiceRegistry
import com.uber.rss.testutil.TestConstants
import org.apache.spark._

object TestUtil {
  def newSparkConfWithStandAloneRegistryServer(appId: String, registryServer: String): SparkConf = {
    new SparkConf().setAppName("testApp")
      .setMaster(s"local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.app.id", appId)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager")
      .set("spark.shuffle.rss.dataCenter", ServiceRegistry.DEFAULT_DATA_CENTER)
      .set("spark.shuffle.rss.cluster", appId)
      .set("spark.shuffle.rss.serviceRegistry.type", ServiceRegistry.TYPE_STANDALONE)
      .set("spark.shuffle.rss.serviceRegistry.server", registryServer)
      .set("spark.shuffle.rss.networkTimeout", "30000")
      .set("spark.shuffle.rss.networkRetries", "0")
      .set("spark.shuffle.rss.maxWaitTime", "10000")
      .set("spark.shuffle.rss.reader.dataAvailableWaitTime", "30000")
  }

  def newSparkConfWithZooKeeperRegistryServer(appId: String, zooKeeperServers: String): SparkConf = {
    val conf = newSparkConfWithStandAloneRegistryServer(appId, "")
    conf.set("spark.shuffle.rss.serviceRegistry.type", ServiceRegistry.TYPE_ZOOKEEPER)
    conf.set("spark.shuffle.rss.serviceRegistry.zookeeper.servers", zooKeeperServers)
    conf
  }
}
