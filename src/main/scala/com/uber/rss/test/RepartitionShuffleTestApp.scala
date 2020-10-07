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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

// This is a basic shuffle application without any aggregation by running a repartition operation.

object RepartitionShuffleTestApp {

  def main (args: Array[String]): Unit = {
    val testApp = new RepartitionShuffleTestApp()
    testApp.run(args)
  }

}

class RepartitionShuffleTestApp extends ShuffleTestAppBaseClass {
  
  def processSourceRdd(currentAppAttempId: String, sourceRdd: RDD[(String, String)]): Long = {
    val rdd = sourceRdd.mapPartitions((records: Iterator[(String, String)]) => {
      records
    }, true)
    
    val shuffledRdd: RDD[(String, String)] = rdd.repartition(numReduces)

    val resultRdd = shuffledRdd.mapPartitions((records: Iterator[(String, String)]) => {
      processPartitionRecords(currentAppAttempId, records)
    })

    val sum = resultRdd.collect().map(_._1).sum
    sum
  }
  
  def processPartitionRecords(currentAppAttempId: String, records: Iterator[(String, String)]) = {
    if (simulateAppAttemptError != null && !simulateAppAttemptError.isEmpty()) {
      val idList = simulateAppAttemptError.split( "," )
      if (idList.contains(currentAppAttempId.toString())) {
        throw new RuntimeException( s"Simulate exception for appAttempId: $currentAppAttempId" )
      } else {
        logInfo( s"Do not simulate exception for appAttempId: $currentAppAttempId (not in list: $simulateAppAttemptError)" )
      }
    }
    val partitionStartTime = System.currentTimeMillis()
    val partitionId = TaskContext.getPartitionId
    var numBytes = 0L
    var numRecords = 0L
    var lastLogTime = 0L
    var lastLogBytes = 0L

    def logStatus() = {
      val duration = System.currentTimeMillis() - lastLogTime
      val throughput = if (duration == 0) {
        "(unknown)"
      } else {
        (numBytes - lastLogBytes).toDouble / (1024 * 1024) / (duration.toDouble / 1000.0) + " mb/s"
      }
      logInfo( s"Partition: $partitionId, verified bytes: $numBytes, records: $numRecords, throughput: $throughput" )
      lastLogTime = System.currentTimeMillis()
      lastLogBytes = numBytes
    }

    while (records.hasNext) {
      if (System.currentTimeMillis() - lastLogTime > 30000) {
        logStatus()
      }

      val next: (String, String) = records.next()
      numBytes = numBytes + next._1.length() + next._2.length()
      numRecords = numRecords + 1
      if (next._1.length() > maxValueLen) {
        throw new RuntimeException( "Invalid key length: " + next._1 )
      }
      if (next._2.length() > maxValueLen) {
        throw new RuntimeException( "Invalid value: " + next._2 )
      }
      if (!next._1.isEmpty()) {
        if (next._1.charAt( 0 ) != next._1.charAt( next._1.length() - 1 )) {
          throw new RuntimeException( "Invalid key (first character not same as last character): " + next._1 )
        }
      }
      if (!next._2.isEmpty()) {
        if (next._2.charAt( 0 ) != next._2.charAt( next._2.length() - 1 )) {
          throw new RuntimeException( "Invalid value (first character not same as last character): " + next._2 )
        }
      }
    }
    totalVerifiedBytes.add( numBytes )
    totalVerifiedRecords.add( numRecords )
    logInfo( s"Partition: $partitionId finished" )
    logStatus()
    val duration = System.currentTimeMillis() - partitionStartTime
    val mbs = if (duration == 0) {
      0
    } else {
      numBytes.toDouble / (1024 * 1024) / (duration.toDouble / 1000.0)
    }
    Seq( (
      numBytes,
      s"partitionId: $partitionId",
      s"partitionStartTime: $partitionStartTime",
      s"duration: $duration",
      s"throughput (mbs): $mbs"
    ) ).iterator
  }
}
