package org.apache.spark.shuffle

import org.apache.spark.SparkConf
import org.scalatest.Assertions._
import org.testng.annotations.Test

@Test
class RssOptsTest {
  private val conf = new SparkConf()

  def testDefaultValues(): Unit = {
    assert(conf.get(RssOpts.dataCenter) === "")
  }

}
