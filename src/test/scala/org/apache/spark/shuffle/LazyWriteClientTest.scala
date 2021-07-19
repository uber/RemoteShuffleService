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

import com.uber.rss.testutil.{RssMiniCluster, StreamServerTestUtils}
import com.uber.rss.StreamServerConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions._
import org.testng.annotations._

import java.util.UUID

class LazyWriteClientTest {

  var appId: String = null
  val numRssServers = 2

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    val streamServerConfig = new StreamServerConfig()
    //    streamServerConfig.setIdleTimeoutMillis(100);
    rssTestCluster = new RssMiniCluster(rootDirs, appId, streamServerConfig)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()
    rssTestCluster.stop()
  }

  /**
   * Connection time out is set to 100 milli seconds. A timeout of 200 milliseconds is deliberately
   * added in mapper task so that application should fail if the connection was established at the
   * start of the mapper task instead of just before sending the shuffle data.
   */
  @Test
  def testLazyClient(): Unit = {
    Seq("false", "true").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.server.createLazyMapperClientConnection", confValue)
      sc = new SparkContext(conf)

      val rdd = sc.parallelize(0 until 10, 1)
        .mapPartitions(partition => {
          Thread.sleep(200)
          partition
        })
        .repartition(2)

      if (confValue == "false") {
        val thrown = intercept[Exception] {
          rdd.collect()
        }
        assert(thrown.getCause.toString.matches("com.uber.rss.exceptions.RssNetworkException: " +
          "writeMessageLengthAndContent failed.*Broken pipe.*"))
      } else {
        val resultSize = rdd.count()
        assert(resultSize == 10)
      }
    })
  }
}