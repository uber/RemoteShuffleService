package org.apache.spark.shuffle

import java.util.UUID

import com.uber.rss.metadata.ServiceRegistry
import com.uber.rss.testutil.{RssMiniCluster, RssZookeeperCluster}
import org.apache.spark.{HashPartitioner, MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkContext, SparkEnv}
import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer

/***
 * This is to test scenario where there is no mapper task (zero number of mappers).
 */
class ZeroMapperShuffleTest {

  var appId: String = null
  val numRssServers = 2
  
  var sc: SparkContext = null
  
  var rssTestCluster: RssMiniCluster = null
  private var shuffleManagers = ArrayBuffer[RssShuffleManager]()

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    shuffleManagers.clear()
    rssTestCluster = new RssMiniCluster(numRssServers, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()
    shuffleManagers.foreach(m => m.stop())
    rssTestCluster.stop()
  }
  
  @Test
  def runTest(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 0
    val partitionId = 0
    val numPartitions = 1

    val rdd = sc.parallelize(Seq.empty[Int], numMaps)
      .map(t=>(t->t*2))
      .partitionBy(new HashPartitioner(numPartitions))

    assert(rdd.partitions.size === numPartitions)

    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle = driverShuffleManager.registerShuffle(shuffleId, numMaps, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= executorShuffleManager

    val reduceTaskContext = new MockTaskContext( shuffleId, partitionId )
    val shuffleReader = executorShuffleManager.getReader( shuffleHandle, partitionId, partitionId, reduceTaskContext )
    val readRecords = shuffleReader.read().toList
    assert( readRecords.size === 0 )
  }

}
