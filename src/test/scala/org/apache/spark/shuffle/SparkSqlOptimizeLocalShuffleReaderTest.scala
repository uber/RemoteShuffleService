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

import java.util.UUID

import com.uber.rss.testutil.{RssMiniCluster, StreamServerTestUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.testng.Assert
import org.testng.annotations._

case class LeftKV(key: Int, value: Int)
case class RightKV(key: Int, value: Int)

class SparkSqlOptimizeLocalShuffleReaderTest {

  var appId: String = null
  val numRssServers = 2

  var spark: SparkSession = null

  var rssTestCluster: RssMiniCluster = null

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()

    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    rssTestCluster = new RssMiniCluster(rootDirs, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    spark.stop()

    rssTestCluster.stop()
  }

  @Test
  def runWithSparkDefaultShuffle(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    conf.set("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.0001")
    conf.set("spark.sql.shuffle.partitions", "4")

    conf.remove("spark.shuffle.manager")

    runWithConf(conf)
  }

  @Test
  def runWithRssShuffle(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    conf.set("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.0001")
    conf.set("spark.sql.shuffle.partitions", "4")

    runWithConf(conf)
  }

  private def runWithConf(conf: SparkConf): Unit = {
    spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val left = spark.sparkContext.parallelize(
      (1 to 10).map(t => LeftKV(t, t)), 2).toDF()
    left.createOrReplaceTempView("left")
    val right = spark.sparkContext.parallelize(Seq(
      RightKV(1, 100), RightKV(1, 101),
      RightKV(2, 200), RightKV(2, 201),
      RightKV(3, 300), RightKV(3, 301)
    ), 2).toDF()
    right.createOrReplaceTempView("right")

    val df = spark.sql("SELECT right.key, right.value FROM left JOIN right ON left.key = right.key WHERE left.value = 1 order by 1, 2")

    val result = df.collect()
    Assert.assertEquals(result.length, 2)

    var row = result(0)
    Assert.assertEquals(row.get(0), 1)
    Assert.assertEquals(row.get(1), 100)

    row = result(1)
    Assert.assertEquals(row.get(0), 1)
    Assert.assertEquals(row.get(1), 101)
  }

}
