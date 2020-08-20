package org.apache.spark.shuffle

import java.util.UUID

import com.uber.rss.testutil.{RssMiniCluster, RssZookeeperCluster, StreamServerTestUtils}
import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkContext}
import org.scalatest.Assertions._
import org.testng.annotations._

/***
 * This is to test scenario where shuffle reader has ordering
 */
class ShuffleWithOrderingTest {

  var appId: String = null
  val numRssServers = 2
  
  var sc: SparkContext = null
  
  var rssTestCluster: RssMiniCluster = null
  
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
  def sortByKey(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    sc = new SparkContext(conf)

    val numValues = 1000
    val numMaps = 3
    val numPartitions = 5

    val rdd = sc.parallelize(0 until numValues, numMaps)
      .map(t=>(t->t*2))
      .sortByKey(ascending = false, numPartitions = numPartitions)
    val result = rdd.collect()

    assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")
    assert(result.size === numValues)

    for (i <- 0 until result.size - 1) {
      val currentRow = result(i)
      val nextRow = result(i + 1)
      assert(currentRow._1 > nextRow._1)
    }
  }

}
