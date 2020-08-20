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

import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, TimeUnit}

import com.uber.rss.clients.RecordWriter
import com.uber.rss.common.{AppTaskAttemptId, ServerDetail, ServerList}
import com.uber.rss.metadata.ServiceRegistry
import com.uber.rss.metrics.ShuffleClientStageMetrics
import org.apache.spark.ShuffleDependency
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.rss.RssUtils

class RssShuffleWriter[K, V, C](
                                 user: String,
                                 rssServers: ServerList,
                                 writeClient: RecordWriter,
                                 mapInfo: AppTaskAttemptId,
                                 numMaps: Int,
                                 serializer: SerializerInstance,
                                 stageAttemptNumber: Int,
                                 shuffleDependency: ShuffleDependency[K, V, C],
                                 stageMetrics: ShuffleClientStageMetrics,
                                 shuffleWriteMetrics: ShuffleWriteMetrics)
    extends ShuffleWriter[K, V] with Logging {

  logInfo(s"Using ShuffleWriter: ${this.getClass.getSimpleName}")

  private val partitioner = shuffleDependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shouldPartition = numPartitions > 1

  private val writeClientCloseLock = new Object()

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    logInfo(s"Writing shuffle records ($mapInfo), map side combine: ${shuffleDependency.mapSideCombine}")

    var numRecords = 0

    val startUploadStartTime = System.nanoTime()
    writeClient.startUpload(mapInfo, numMaps, numPartitions)
    val startUploadTime = System.nanoTime() - startUploadStartTime

    var writeRecordTime = 0L
    var serializeTime = 0L

    var recordFetchStartTime = System.nanoTime()
    var recordFetchTime = 0L

    while (records.hasNext) {
      val record = records.next()
      recordFetchTime += (System.nanoTime() - recordFetchStartTime)

      val writeRecordStartTime = System.nanoTime()

      val partition = getPartition(record._1)

      if (shuffleDependency.mapSideCombine) {
        val createCombiner = shuffleDependency.aggregator.get.createCombiner
        val c = createCombiner(record._2)
        val serializeStartTime = System.nanoTime()
        val serializedRecord = serializeRecord((record._1, c))
        serializeTime += (System.nanoTime() - serializeStartTime)
        writeClient.sendRecord(partition, serializedRecord._1, serializedRecord._2)
      } else {
        val serializeStartTime = System.nanoTime()
        val serializedRecord = serializeRecord(record)
        serializeTime += (System.nanoTime() - serializeStartTime)
        writeClient.sendRecord(partition, serializedRecord._1, serializedRecord._2)
      }

      numRecords = numRecords + 1
      writeRecordTime += (System.nanoTime() - writeRecordStartTime)

      recordFetchStartTime = System.nanoTime()
    }

    val finishUploadStartTime = System.nanoTime()
    writeClient.finishUpload()
    val finishUploadTime = System.nanoTime() - finishUploadStartTime

    val totalBytes = writeClient.getShuffleWriteBytes()
    logInfo(s"Wrote shuffle records ($mapInfo), $numRecords records, $totalBytes bytes, write seconds: ${TimeUnit.NANOSECONDS.toSeconds(startUploadTime)}, ${TimeUnit.NANOSECONDS.toSeconds(writeRecordTime)}, ${TimeUnit.NANOSECONDS.toSeconds(finishUploadTime)}, serialize seconds: ${TimeUnit.NANOSECONDS.toSeconds(serializeTime)}, record fetch seconds: ${TimeUnit.NANOSECONDS.toSeconds(recordFetchTime)}")

    shuffleWriteMetrics.incRecordsWritten(numRecords)
    shuffleWriteMetrics.incBytesWritten(totalBytes)
    shuffleWriteMetrics.incWriteTime(startUploadTime + writeRecordTime + finishUploadTime)

    closeWriteClientAsync()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo(s"Stopped shuffle writer ($mapInfo), success: $success")

    closeWriteClientAsync()

    if (success) {
      // fill partitionLengths with non zero dummy value so map output tracker could work correctly
      val partitionLengths: Array[Long] = Array.fill(numPartitions)(1L)
      val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, stageAttemptNumber, rssServers)
      Some(MapStatus(blockManagerId, partitionLengths))
    } else {
      None
    }
  }

  private def serializeRecord(record: Product2[K, _]) = {
    val key: ByteBuffer = if (record._1 == null) {
      null
    } else {
      serializer.serialize(record._1.asInstanceOf[Any])
    }

    val value: ByteBuffer = if (record._2 == null) {
      null
    } else {
      serializer.serialize(record._2)
    }

    (key, value)
  }

  private def closeWriteClientAsync() = {
    CompletableFuture.runAsync(new Runnable {
      override def run(): Unit = {
        writeClientCloseLock.synchronized {
          writeClient.close()
        }
      }
    })
  }
}
