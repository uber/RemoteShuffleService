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

import com.uber.rss.testutil.RssZookeeperCluster
import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.rss.RssUtils
import org.scalatest.Assertions._
import org.testng.annotations._

class RssShuffleManagerServerRestartTest {

  var appId: String = null
  val numRssServers = 2
  
  var sc: SparkContext = null
  
  var rssTestCluster: RssZookeeperCluster = null
  
  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    rssTestCluster = new RssZookeeperCluster(numRssServers, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()
    rssTestCluster.stop()
  }

  @Test
  def restartServerBeforeShuffleWrite(): Unit = {
    val conf = TestUtil.newSparkConfWithZooKeeperRegistryServer(appId, rssTestCluster.getZooKeeperServers)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    sc = new SparkContext(conf)

    assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")

    val shuffleId = 1
    val numMaps = 10
    val numValuesInMap = 100
    val numPartitions = 5

    val rdd = sc.parallelize(1 to 100)
      .map(t=>(t->t*2))
      .partitionBy(new HashPartitioner(numPartitions))
    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle = sc.env.shuffleManager.registerShuffle(shuffleId, numMaps, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps)

    // restart rss server
    rssTestCluster.restartShuffleServers()

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)

    (0 until numMaps).toList.par.map(mapId => {
      val taskAttemptId = mapId + 1000
      val mapTaskContext = new MockTaskContext(shuffleId, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager.getWriter[Int, Int]( shuffleHandle, mapId, mapTaskContext )
      val records = (1 to numValuesInMap).map(t => (mapId*1000+t) -> (mapId*1000+t*2)).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)
    })
  }
  
  @Test
  def restartServerAfterShuffleWrite(): Unit = {
    val conf = TestUtil.newSparkConfWithZooKeeperRegistryServer(appId, rssTestCluster.getZooKeeperServers)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    sc = new SparkContext(conf)

    assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")

    val numValues = 1000
    val numMaps = 3
    val numPartitions = 5

    val rdd = sc.parallelize(0 until numValues, numMaps)
      .map(t=>((t/2) -> (t*2).longValue()))
      .foldByKey(0, numPartitions)((v1, v2)=>v1 + v2)

    var result = rdd.collect()
    assert(result.size === numValues/2)

    for (i <- 0 until result.size) {
      val key = result(i)._1
      val value = result(i)._2
      assert(key*2*2 + (key*2+1)*2 === value)
    }

    var keys = result.map(_._1).distinct.sorted
    assert(keys.length === numValues/2)
    assert(keys(0) === 0)
    assert(keys.last === (numValues-1)/2)

    // restart rss server
    rssTestCluster.restartShuffleServers()

    result = rdd.collect()
    assert(result.size === numValues/2)

    for (i <- 0 until result.size) {
      val key = result(i)._1
      val value = result(i)._2
      assert(key*2*2 + (key*2+1)*2 === value)
    }

    keys = result.map(_._1).distinct.sorted
    assert(keys.length === numValues/2)
    assert(keys(0) === 0)
    assert(keys.last === (numValues-1)/2)
  }

  @Test
  def restartServer_firstStageWriteNotFinished_secondStageWriteFinished_readSecondStage(): Unit = {
    val conf = TestUtil.newSparkConfWithZooKeeperRegistryServer(appId, rssTestCluster.getZooKeeperServers)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)

    val shuffleId1 = 1
    val numMaps1 = 10

    val numValuesInMap = 100
    val numPartitions = 5

    val shuffleId2 = 2
    val numMaps2 = 20

    val rdd = sc.parallelize(1 to 100)
      .map(t=>(t->t*2))
      .partitionBy(new HashPartitioner(numPartitions))
    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle1 = driverShuffleManager.registerShuffle(shuffleId1, numMaps1, shuffleDependency)
    val shuffleHandle2 = driverShuffleManager.registerShuffle(shuffleId2, numMaps2, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId1, numMaps1)
    mapOutputTrackerMaster.registerShuffle(shuffleId2, numMaps2)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)

    // write shuffle stage 1 data with (numMaps1 - 1) map tasks and leave last map task not writing data for testing purpose
    // will test reading this shuffle stage after rss server restart and it should fail
    (0 until numMaps1 - 1).toList.par.map(mapId => {
      val taskAttemptId = mapId + 1000
      val mapTaskContext = new MockTaskContext(shuffleId1, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager.getWriter[Int, Int]( shuffleHandle1, mapId, mapTaskContext )
      val records = (1 to numValuesInMap).map(t => (mapId*1000+t) -> (mapId*1000+t*2)).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId1, mapId, mapStatus)
    })

    // register map status for last map task to simulate that map task finishes writing but shuffle server does not flush data
    val lastMapId = numMaps1 - 1
    val lastMapTaskAttemptId = lastMapId + 1000
    val partitionLengths: Array[Long] = Array.fill(numPartitions)(1L)
    val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(lastMapId, lastMapTaskAttemptId, shuffleHandle1.asInstanceOf[RssShuffleHandle[_, _, _]].getServerList)
    val lastMapStatus = MapStatus(blockManagerId, partitionLengths)
    mapOutputTrackerMaster.registerMapOutput(shuffleId1, numMaps1 - 1, lastMapStatus)

    // write shuffle stage 2 data
    (0 until numMaps2).toList.par.map(mapId => {
      val taskAttemptId = mapId + 2000
      val mapTaskContext = new MockTaskContext(shuffleId2, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager.getWriter[Int, Int]( shuffleHandle2, mapId, mapTaskContext )
      val records = (1 to numValuesInMap).map(t => (mapId*1000+t) -> (mapId*1000+t*2)).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId2, mapId, mapStatus)
    })

    // read shuffle stage 2 data to make sure rss server flushed data to disk
    {
      val startPartition = 0
      val endPartition = 0
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 2
      val reduceTaskContext = new MockTaskContext( shuffleId1, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === 2 * numMaps2 * numValuesInMap / numPartitions )
    }

    // restart rss servers
    rssTestCluster.restartShuffleServers()

    // read shuffle stage 2 data
    {
      val startPartition = 0
      val endPartition = 0
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 2
      val reduceTaskContext = new MockTaskContext( shuffleId1, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === 2 * numMaps2 * numValuesInMap / numPartitions )
    }
  }

  @Test(expectedExceptions = Array(classOf[FetchFailedException]))
  def restartServer_firstStageWriteNotFinished_secondStageWriteFinished_readFirstStage(): Unit = {
    val conf = TestUtil.newSparkConfWithZooKeeperRegistryServer(appId, rssTestCluster.getZooKeeperServers)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.reader.dataAvailableWaitTime", "1000")
    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)

    val shuffleId1 = 1
    val numMaps1 = 10

    val numValuesInMap = 100
    val numPartitions = 5

    val shuffleId2 = 2
    val numMaps2 = 20

    val rdd = sc.parallelize(1 to 100)
      .map(t=>(t->t*2))
      .partitionBy(new HashPartitioner(numPartitions))
    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle1 = driverShuffleManager.registerShuffle(shuffleId1, numMaps1, shuffleDependency)
    val shuffleHandle2 = driverShuffleManager.registerShuffle(shuffleId2, numMaps2, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId1, numMaps1)
    mapOutputTrackerMaster.registerShuffle(shuffleId2, numMaps2)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)

    // write shuffle stage 1 data with (numMaps1 - 1) map tasks and leave last map task not writing data for testing purpose
    // will test reading this shuffle stage after rss server restart and it should fail
    (0 until numMaps1 - 1).toList.par.map(mapId => {
      val taskAttemptId = mapId + 1000
      val mapTaskContext = new MockTaskContext(shuffleId1, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager.getWriter[Int, Int]( shuffleHandle1, mapId, mapTaskContext )
      val records = (1 to numValuesInMap).map(t => (mapId*1000+t) -> (mapId*1000+t*2)).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId1, mapId, mapStatus)
    })

    // register map status for last map task to simulate that map task finishes writing but shuffle server does not flush data
    val lastMapId = numMaps1 - 1
    val lastMapTaskAttemptId = lastMapId + 1000
    val partitionLengths: Array[Long] = Array.fill(numPartitions)(1L)
    val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(lastMapId, lastMapTaskAttemptId, shuffleHandle1.asInstanceOf[RssShuffleHandle[_, _, _]].getServerList)
    val lastMapStatus = MapStatus(blockManagerId, partitionLengths)
    mapOutputTrackerMaster.registerMapOutput(shuffleId1, numMaps1 - 1, lastMapStatus)

    // write shuffle stage 2 data
    (0 until numMaps2).toList.par.map(mapId => {
      val taskAttemptId = mapId + 2000
      val mapTaskContext = new MockTaskContext(shuffleId2, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager.getWriter[Int, Int]( shuffleHandle2, mapId, mapTaskContext )
      val records = (1 to numValuesInMap).map(t => (mapId*1000+t) -> (mapId*1000+t*2)).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId2, mapId, mapStatus)
    })

    // read shuffle stage 2 data to make sure rss server flushed data to disk
    {
      val startPartition = 0
      val endPartition = 0
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext( shuffleId2, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps2 * numValuesInMap / numPartitions )
    }
    {
      val startPartition = 0
      val endPartition = 2
      val reduceTaskContext = new MockTaskContext( shuffleId1, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle2, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === 2 * numMaps2 * numValuesInMap / numPartitions )
    }

    // restart rss servers
    rssTestCluster.restartShuffleServers()

    // read shuffle stage 1 data, should fail
    {
      val startPartition = 0
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext( shuffleId1, startPartition )
      val shuffleReader = executorShuffleManager.getReader( shuffleHandle1, startPartition, endPartition, reduceTaskContext )
      val readRecords = shuffleReader.read().toList
      assert( readRecords.size === numMaps1 * numValuesInMap / numPartitions )
    }
  }
}
