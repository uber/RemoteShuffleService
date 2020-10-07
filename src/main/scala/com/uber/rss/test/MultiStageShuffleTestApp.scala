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
import org.apache.spark.sql.SparkSession

/**
 * This is a Spark application to run multiple stages with sleeping in each stage
 * to control how long each stage runs.
 */
object MultiStageShuffleTestApp {

  def main(args: Array[String]): Unit = {
    val testApp = new MultiStageShuffleTestApp()
    testApp.run(args)
  }

}

class MultiStageShuffleTestApp {

  def run(args: Array[String]): Unit = {
    var localRun = false
    var numMaps = 4
    var stage1Seconds = 1
    var stage2Seconds = 1

    var i = 0
    while (i < args.length) {
      val argName = args(i)
      i += 1
      if (argName.equalsIgnoreCase("-local")) {
        localRun = args(i).toBoolean
        i += 1
      } else if (argName.equalsIgnoreCase("-numMaps")) {
        numMaps = args(i).toInt
        i += 1
        System.out.println(s"numMaps: $numMaps")
      } else if (argName.equalsIgnoreCase("-stage1Seconds")) {
        stage1Seconds = args(i).toInt
        i += 1
        System.out.println(s"stage1Seconds: $stage1Seconds")
      } else if (argName.equalsIgnoreCase("-stage2Seconds")) {
        stage2Seconds = args(i).toInt
        i += 1
        System.out.println(s"stage2Seconds: $stage2Seconds")
      } else {
        throw new RuntimeException(s"Invalid argument: $argName")
      }
    }

    val conf = new SparkConf()
      .setAppName(this.getClass().getSimpleName())

    if (localRun) {
      conf.setMaster("local")
    }

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val numberRdd = spark.sparkContext.parallelize(0 until numMaps, numMaps)
    val rdd1 = numberRdd.map(t => (t / 2, t * 1000))
      .mapPartitions(iter => {
        System.out.println(s"Sleeping $stage1Seconds seconds in stage 1")
        Thread.sleep(stage1Seconds * 1000)
        iter
      })
    val rdd2 = rdd1.repartition(numMaps / 2)
      .map(t => (t._1 / 2, t._2))
      .mapPartitions(iter => {
        System.out.println(s"Sleeping $stage2Seconds seconds in stage 2")
        Thread.sleep(stage2Seconds * 1000)
        iter
      })

    val result = rdd2.collect()

    if (result.length != numMaps) {
      throw new RuntimeException(s"Application error, result length: ${result.length}, expected: ${numMaps / 4}")
    }

    val min: (Int, Int) = result.minBy(_._2)
    val minExpectedValue: (Int, Int) = (0, 0)
    if (!min.equals(minExpectedValue)) {
      throw new RuntimeException(s"Application error, min value: $min, expected: $minExpectedValue")
    }

    val max = result.maxBy(_._2)
    val maxExpectedValue = ((numMaps-1)/4, (numMaps-1)*1000)
    if (!max.equals(maxExpectedValue)) {
      throw new RuntimeException(s"Application error, max value: $max, expected: $maxExpectedValue")
    }

    spark.stop()
  }

}

