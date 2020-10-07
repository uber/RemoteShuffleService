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

package com.uber.rss.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

// This Spark application tests Spark RDD with aggregation and ordering

object AggOrderingShuffleTestApp {

  def main (args: Array[String]): Unit = {
    var localRun = false
    var numMaps = 10
    var numValues = 10000
    var numReduces = 20

    var i = 0
    while (i < args.length) {
      val argName = args( i )
      i += 1
      if (argName.equalsIgnoreCase("-local")) {
        localRun = args(i).toBoolean
        i += 1
      } else if (argName.equalsIgnoreCase( "-numMaps" )) {
        numMaps = args( i ).toInt
        i += 1
      } else if (argName.equalsIgnoreCase( "-numValues" )) {
        numValues = args( i ).toInt
        i += 1
      } else if (argName.equalsIgnoreCase( "-numReduces" )) {
        numReduces = args( i ).toInt
        i += 1
      } else {
        throw new RuntimeException( s"Invalid argument: $argName" )
      }
    }

    val conf = new SparkConf()
      .setAppName(AggOrderingShuffleTestApp.getClass().getSimpleName())

    if (localRun) {
      conf.setMaster("local")
    }

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    System.out.println(s"Generating $numValues unique numbers")

    val numKeys = numReduces * 3

    val allValues = scala.collection.mutable.SortedSet[Long]()
    // add values to make sure there is as least one value for each key
    for (i <- (0 until numKeys)) {
      allValues += i
    }
    // add more random values
    val random = new Random()
    while (allValues.size < numValues) {
      allValues += random.nextInt(Integer.MAX_VALUE)
    }

    System.out.println(s"Generated ${allValues.size} unique numbers")

    val rdd = spark.sparkContext.parallelize(allValues.toSeq, numMaps)

    val resultRdd: RDD[(Long, Long)] = rdd.map(t=>(t%numKeys, t))
      .repartition(numReduces)
      .foldByKey(0L)((v1, v2)=>v1 + v2)
      .sortBy(_._2)

    val result = resultRdd.collect()

    System.out.println(s"Result row count: ${result.length}, first row: ${result(0)}, last row: ${result.last}")

    if (result.length != numKeys) {
      throw new RuntimeException(s"Invalid row count, expected: $numKeys, actual: ${result.length}")
    }

    val totalSumInResult = result.map(_._2).sum
    val sumOfOriginalValues = allValues.sum
    if (totalSumInResult != sumOfOriginalValues) {
      throw new RuntimeException(s"Invalid total sum, expected: $sumOfOriginalValues, actual: $totalSumInResult");
    }

    for (i <- 0 until result.length - 1) {
      val currentRow = result(i)
      val nextRow = result(i + 1)
      if (currentRow._2 > nextRow._2) {
        throw new RuntimeException(s"Invalid ordering for row $i ($currentRow) and record ${i+1} ($nextRow)")
      }
    }

    spark.stop()
  }
}