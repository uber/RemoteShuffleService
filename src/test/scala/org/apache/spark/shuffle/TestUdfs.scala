package org.apache.spark.shuffle

import java.util.Random

import org.apache.commons.lang3.StringUtils

object TestUdfs {
  val random = new Random()

  val testValues = (1 to 1000).map(n => {
    StringUtils.repeat('a', random.nextInt(n))
  }).toList

  def intToString(intValue: Int): String = {
    "%09d".format(intValue)
  }

  def generateString(size: Int): String = {
    StringUtils.repeat('a', size)
  }

  def randomString(): String = {
    val index = random.nextInt(testValues.size)
    testValues(index)
  }
}
