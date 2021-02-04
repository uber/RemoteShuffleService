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
import com.uber.rss.testutil.{RssMiniCluster, RssZookeeperCluster, StreamServerTestUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.SparkContext
import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer

/***
 * This is to test shuffle with aggregation
 */
class ShuffleWithAggregationTest {

  var appId: String = null
  val numRssServers = 2
  
  var sc: SparkContext = null
  
  var rssTestCluster: RssMiniCluster = null

  val globalConf = List("spark.sql.autoBroadcastJoinThreshold", "-1")
  
  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()

    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    rssTestCluster = new RssMiniCluster(rootDirs, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()

    rssTestCluster.stop()
  }

  private def runAndReturnMetrics(job: => Unit,
                                  collector: SparkListenerTaskEnd => Long):
  Long = {
    val taskMetrics = new ArrayBuffer[Long]()

    // Avoid receiving earlier taskEnd events
    sc.listenerBus.waitUntilEmpty(500)

    sc.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = collector(taskEnd)
        taskMetrics.append(metrics)
      }
    })

    job

    sc.listenerBus.waitUntilEmpty(500)
    taskMetrics.sum
  }

  @Test
  def foldByKey(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numMaps = 3
      val numPartitions = 5

      val rdd = sc.parallelize(0 until numValues, numMaps)
        .map(t=>((t/2) -> (t*2).longValue()))
        .foldByKey(0, numPartitions)((v1, v2)=>v1 + v2)
      val result = rdd.collect()

      assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")
      assert(result.size === numValues/2)

      for (i <- 0 until result.size) {
        val key = result(i)._1
        val value = result(i)._2
        assert(key*2*2 + (key*2+1)*2 === value)
      }

      val keys = result.map(_._1).distinct.sorted
      assert(keys.length === numValues/2)
      assert(keys(0) === 0)
      assert(keys.last === (numValues-1)/2)
    })
  }

  @Test
  def reduceByKey(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numMaps = 3
      val numPartitions = 5

      val rdd = sc.parallelize(0 until numValues, numMaps)
        .map(t=>((t/2) -> (t*2).longValue()))
        .reduceByKey(_ + _)
      val result = rdd.collect()

      assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")
      assert(result.size === numValues/2)

      for (i <- 0 until result.size) {
        val key = result(i)._1
        val value = result(i)._2
        assert(key*2*2 + (key*2+1)*2 === value)
      }

      val keys = result.map(_._1).distinct.sorted
      assert(keys.length === numValues/2)
      assert(keys(0) === 0)
      assert(keys.last === (numValues-1)/2)
    })
  }

  @Test
  def recordsNoAggregation(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "false")
      conf.set("spark.shuffle.rss.mapSideAggregation.dynamicAllocation.enabled", confValue)
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numPartitions = 10

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 1000)
      assert(shuffleRecordsRead == 1000)
    })
  }

  @Test
  def recordsWrittenPartialAggregation(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.dynamicAllocation.enabled", confValue)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "true")
      conf.set("spark.shuffle.rss.mapSideAggregation.reductionFactorBackoffMinRecords", "10")
      conf.set("spark.shuffle.rss.mapSideAggregation.reductionFactorBackoffThreshold", "1.0")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numPartitions = 10

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .mapPartitions(partition => {
          partition.toSeq.sorted.iterator
        })
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      // since the backoff sample size is 10, first 10 records in each partition will be aggregated and then the
      // rest records will just be type casted as the reduction factor is set to 1.0
      assert(shuffleRecordsWritten == 950)
      assert(shuffleRecordsRead == 950)
    })
  }

  @Test
  def recordsWrittenCompleteAggregation(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.dynamicAllocation.enabled", confValue)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "true")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numPartitions = 10

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 500)
      assert(shuffleRecordsRead == 500)
    })
  }

  @Test
  def dynamicMemoryAllocationTriggers(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.mapSideAggregation.dynamicAllocation.enabled", "true")
    conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "true")
    // Allocate very little memory so that dynamic allocation gets triggered
    conf.set("spark.shuffle.rss.spill.initialMemoryThreshold", "5")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sc = new SparkContext(conf)

    val numValues = 1000
    val numPartitions = 10

    val rdd = sc.parallelize(0 until numValues, numPartitions)
      .map(t => ((t / 2) -> (t * 2).longValue()))
      .reduceByKey(_ + _)

    val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
    val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
    assert(shuffleRecordsWritten == 500)
    assert(shuffleRecordsRead == 500)
  }
}
