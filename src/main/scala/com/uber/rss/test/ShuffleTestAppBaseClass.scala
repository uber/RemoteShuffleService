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

import java.nio.file.Paths
import java.util.UUID

import com.uber.rss.test.util.KeyValueGenerator
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, TaskContext}

abstract class ShuffleTestAppBaseClass extends Serializable with Logging {
  var localRun = false
  var tempDir = "/tmp/" + this.getClass().getSimpleName
  var numIterations = 1
  var numMaps = 10
  var mapBytes = 1024L*1024L
  var numReduces = 20
  var numValues = 1000
  var maxValueLen = 10000
  var simulateAppAttemptError = ""
  var simulateTaskAttemptError = ""
  
  var totalGeneratedBytes: LongAccumulator = null
  var totalGeneratedRecords: LongAccumulator = null
  var totalVerifiedBytes: LongAccumulator = null
  var totalVerifiedRecords: LongAccumulator = null
  
  def processArgs(args: Array[String]): Unit = {
    var i = 0
    while (i < args.length) {
      val argName = args( i )
      i += 1
      if (argName.equalsIgnoreCase("-local")) {
        localRun = args(i).toBoolean
        i += 1
      } else if (argName.equalsIgnoreCase( "-tempDir" )) {
        tempDir = args( i )
        i += 1
        logInfo( s"tempDir: $tempDir" )
      } else if (argName.equalsIgnoreCase( "-numIterations" )) {
        numIterations = args( i ).toInt
        i += 1
        logInfo( s"numIterations: $numIterations" )
      } else if (argName.equalsIgnoreCase( "-numMaps" )) {
        numMaps = args( i ).toInt
        i += 1
        logInfo( s"numMaps: $numMaps" )
      } else if (argName.equalsIgnoreCase( "-mapBytes" )) {
        mapBytes = args( i ).toLong
        i += 1
        logInfo( s"mapBytes: $mapBytes" )
      } else if (argName.equalsIgnoreCase( "-numReduces" )) {
        numReduces = args( i ).toInt
        i += 1
        logInfo( s"numReduces: $numReduces" )
      } else if (argName.equalsIgnoreCase( "-simulateAppAttemptError" )) {
        simulateAppAttemptError = args( i )
        i += 1
        logInfo( s"simulateAppAttemptError: $simulateAppAttemptError" )
      } else if (argName.equalsIgnoreCase( "-simulateTaskAttemptError" )) {
        simulateTaskAttemptError = args( i )
        i += 1
        logInfo( s"simulateTaskAttemptError: $simulateTaskAttemptError" )
      }  else {
        throw new RuntimeException( s"Invalid argument: $argName" )
      }
    }
  }

  def postSparkConf(sparkConf: SparkConf): SparkConf = {
    sparkConf
  }

  def createAccumulators(spark: SparkSession): Unit = {
    totalGeneratedBytes = spark.sparkContext.longAccumulator("totalGeneratedBytes")
    totalGeneratedRecords = spark.sparkContext.longAccumulator("totalGeneratedRecords")
    
    totalVerifiedBytes = spark.sparkContext.longAccumulator("totalVerifiedBytes")
    totalVerifiedRecords = spark.sparkContext.longAccumulator("totalVerifiedRecords")
  }

  def processSourceRdd(currentAppAttempId: String, sourceRdd: RDD[(String, String)]): Long

  def run(args: Array[String]): Unit = {

    processArgs(args)

    var conf = new SparkConf()
      .setAppName(this.getClass().getSimpleName())

    if (localRun) {
      conf.setMaster("local")
    }

    conf = postSparkConf(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    createAccumulators(spark)

    val appAttempId = spark.sparkContext.applicationAttemptId.getOrElse("")
    logInfo(s"appAttempId: $appAttempId")

    var sourceRdd: RDD[(String, String)] = createSourceRdd(spark)

    (0 until numIterations).foreach(i => {
      val startTime = System.currentTimeMillis()
      val sum = processSourceRdd( appAttempId, sourceRdd )
      val duration = System.currentTimeMillis() - startTime

      if (i == 0) {
        verifyAndPrintResult( startTime, duration )
      }
    })

    // Stop SparkContext before the job is done
    spark.stop()
  }
  
  def createSourceRdd(spark: SparkSession): RDD[(String, String)] = {
    val rdd = spark.sparkContext.parallelize(0 until numMaps, numMaps)
    
    var sourceRdd: RDD[(String, String)] = rdd.mapPartitions(iter => {
      val taskAttemptId = TaskContext.get().taskAttemptId()
      logInfo(s"taskAttemptId: $taskAttemptId")
      if (simulateTaskAttemptError != null && !simulateTaskAttemptError.isEmpty()) {
        val idList = simulateTaskAttemptError.split(",")
        if (idList.contains(taskAttemptId.toString())) {
          throw new RuntimeException(s"Simulate exception for taskAttemptId: $taskAttemptId")
        } else {
          logInfo(s"Do not simulate exception for taskAttemptId: $taskAttemptId (not in list: $simulateTaskAttemptError)")
        }
      }
      val partitionId = TaskContext.getPartitionId
      val startTime = System.currentTimeMillis()
      val random = new java.util.Random()
      val testValues = scala.collection.mutable.ArrayBuffer.empty[String]
      while (testValues.size < numValues) {
        val ch = ('a' + random.nextInt(26)).toChar
        val repeats = random.nextInt(maxValueLen)
        val str = StringUtils.repeat(ch, repeats)
        testValues += str
      }
      val durationGenerateTestValues = System.currentTimeMillis() - startTime
      logInfo(s"Duration to generate test values: $durationGenerateTestValues")
      new KeyValueGenerator(partitionId, testValues, mapBytes, totalGeneratedBytes, totalGeneratedRecords)
    })

    val fileDir = Paths.get(tempDir, UUID.randomUUID().toString).toString

    System.out.println(s"Saving source RDD to file: $fileDir")
    
    spark.createDataFrame(sourceRdd)
      .write.format("parquet").mode("overwrite").save(fileDir)

    sourceRdd = spark.read.format("parquet").load(fileDir)
      .rdd.map(row=>(row.getString(0), row.getString(1))).coalesce(numMaps)

    sourceRdd = sourceRdd.persist(StorageLevel.DISK_ONLY)

    val fileDirPath = new Path(fileDir)
    val fs = fileDirPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.deleteOnExit(fileDirPath)
    System.out.println(s"Delete file on exit: $fileDir")

    sourceRdd = sourceRdd.mapPartitions((records: Iterator[(String, String)]) => {
      val partitionId = TaskContext.getPartitionId
      new LoggedIterator(records, partitionId)
    }, true)
    
    sourceRdd
  }
  
  def verifyAndPrintResult(startTime: Long, duration: Long) = {
    val str = s"Total maps: $numMaps, total generated bytes: ${totalGeneratedBytes.value}, records: ${totalGeneratedRecords.value}, total verified bytes: ${totalVerifiedBytes.value}, records: ${totalVerifiedRecords.value}, duration millis: $duration"
    logInfo(str)

    if (totalGeneratedBytes.value != totalVerifiedBytes.value) {
      throw new RuntimeException("Bytes verification failed: " + str);
    }

    if (totalGeneratedRecords.value != totalVerifiedRecords.value) {
      throw new RuntimeException("Records verification failed: " + str);
    }
  }
}

class LoggedIterator(underlyingIterator: Iterator[(String, String)], partitionId: Int) 
  extends Iterator[(String, String)] with Logging {
  var startTime = 0L
  var totalBytes = 0L
  
  override def hasNext: Boolean = {
    if (startTime == 0) {
      startTime = System.currentTimeMillis()
    }
    
    val result = underlyingIterator.hasNext
    if (!result) {
      val duration = System.currentTimeMillis() - startTime
      val throughput = if (duration == 0) {
        "(unknown)"
      } else {
        totalBytes.toDouble / (1024 * 1024) / (duration.toDouble / 1000.0) + " mb/s"
      }
      logInfo(s"Partition $partitionId, iterator throughput: $throughput")
    }
    
    result
  }

  override def next(): (String, String) = {
    val result = underlyingIterator.next()
    if (result != null) {
      if (result._1 != null) {
        totalBytes = totalBytes + result._1.size
      }
      if (result._2 != null) {
        totalBytes = totalBytes + result._2.size
      }
    }
    result
  }
}