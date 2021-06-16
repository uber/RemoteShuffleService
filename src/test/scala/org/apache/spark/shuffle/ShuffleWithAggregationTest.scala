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

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.{SparkContext, TestUtils}

import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/***
 * This is to test shuffle with aggregation
 */
class ShuffleWithAggregationTest {

  var appId: String = null
  val numRssServers = 2

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null

  private def runAndReturnMetrics(job: => Unit,
                                  collector: SparkListenerTaskEnd => Long): Long = {
    val taskMetrics = new ArrayBuffer[Long]()

    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = collector(taskEnd)
        taskMetrics.append(metrics)
      }
    }

    TestUtils.withListener(sc, listener) {_ => job}

    taskMetrics.sum
  }

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

  @Test
  def foldByKey(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
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

  /**
   * There are two records for each key in the dataset. Those should be combined
   * with map side aggregation or without map side aggregation (on reducer side).
   */
  @Test
  def reduceByKey(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
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

  /**
   * With map side aggregation disabled, there should be no reduction on mapper side, so number
   * of written by mapper should be equal to number of records read.
   */
  @Test
  def recordsNoAggregation(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "false")
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
  }

  /**
   * With map side aggregation enabled and low threshold memory allocated, map side aggregation would
   * not be complete and only some of the record should get aggregated.
   */
  @Test
  def recordsWrittenPartialAggregation(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "true")
    conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "2000")
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
    assert(shuffleRecordsWritten < 1000)
    assert(shuffleRecordsRead < 1000)
  }

  /**
   * With map side aggregation enabled and enough memory threshold to fit all the data in memory,
   * map side aggregation should be complete.
   */
  @Test
  def recordsWrittenCompleteAggregation(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.mapSideAggregation.enabled", "true")
    conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "5242880")
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
    assert(shuffleRecordsWritten == 500)
    assert(shuffleRecordsRead == 500)
  }

  /**
   * Each partition here would read 100 records. All the key values are the same, so with map side aggregation
   * each mapper should only write 1 record, so 10 (1 * 10 partitions) records should be written in mapper stage.
   */
  @Test
  def customAggregation(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "5242880")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1000
      val numPartitions = 10

      val list = List.fill(numValues)(0)

      val rdd = sc.parallelize(list, numPartitions)
        .map(t => t -> t.longValue)

      val rddCombined = rdd.combineByKey(
        (i: Long) => mutable.Set(i),
        (set: mutable.Set[Long], i: Long) => set += i,
        (set1: mutable.Set[Long], set2: mutable.Set[Long]) => set1 ++= set2)

      val shuffleRecordsWritten = runAndReturnMetrics(rddCombined.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rddCombined.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)

      if (confValue == "true") {
        // Since all keys are same, only one record should be written per partition
        assert(shuffleRecordsWritten == 10)
        assert(shuffleRecordsRead == 10)
      } else {
        assert(shuffleRecordsWritten == 1000)
        assert(shuffleRecordsRead == 1000)
      }
    })
  }

  /**
   * Test for case when all partitions are empty.
   */
  @Test
  def aggregationNoRecords(): Unit = {
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "5242880")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 0
      val numPartitions = 10

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .mapPartitions(partition => {
          partition.toSeq.sorted.iterator
        })
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 0)
      assert(shuffleRecordsRead == 0)
    })
  }

  /**
   * Test for case when only 1 partition has data and all others are empty.
   */
  @Test
  def aggregationSingleRecordEmptyPartitions(): Unit = {
    // Only one partition has data, rest all partitions are empty
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "5242880")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1
      val numPartitions = 10

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .mapPartitions(partition => {
          partition.toSeq.sorted.iterator
        })
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 1)
      assert(shuffleRecordsRead == 1)
    })
  }

  /**
   * Assign just enough memory that spill happens only once after all records are processed.
   */
  @Test
  def aggregationJustEnoughMemory(): Unit = {
    // Assign just enough memory that spill happens only once all records are processed
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      // Size for the 1 records gets estimated to be around 920
      conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "921")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 1
      val numPartitions = 1

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .mapPartitions(partition => {
          partition.toSeq.sorted.iterator
        })
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 1)
      assert(shuffleRecordsRead == 1)
    })
  }

  /**
   * Assign just enough memory that spill happens before all records are processed.
   */
  @Test
  def aggregationJustNotEnoughMemory(): Unit = {
    // Assign just enough memory that spill happens just before last record is processed and
    // and the last record has to be written to RSS as part of the post record processing spill
    Seq("true", "false").foreach(confValue => {
      val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
      conf.set("spark.shuffle.rss.mapSideAggregation.enabled", confValue)
      // Size for the 1 records gets estimated to be around 920
      conf.set("spark.shuffle.rss.mapSideAggregation.initialMemoryThresholdInBytes", "920")
      conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      sc = new SparkContext(conf)

      val numValues = 2
      val numPartitions = 1

      val rdd = sc.parallelize(0 until numValues, numPartitions)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .mapPartitions(partition => {
          partition.toSeq.sorted.iterator
        })
        .reduceByKey(_ + _)

      val shuffleRecordsWritten = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleWriteMetrics.recordsWritten)
      val shuffleRecordsRead = runAndReturnMetrics(rdd.collect(), _.taskMetrics.shuffleReadMetrics.recordsRead)
      assert(shuffleRecordsWritten == 2)
      assert(shuffleRecordsRead == 2)
    })
  }
}