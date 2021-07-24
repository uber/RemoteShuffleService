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

import java.util.concurrent.TimeUnit
import com.uber.rss.clients.ShuffleDataWriter
import com.uber.rss.common.{AppTaskAttemptId, ServerList}
import com.uber.rss.exceptions.RssInvalidStateException
import com.uber.rss.metrics.{M3Stats, ShuffleClientStageMetrics}
import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.rss.{BufferManagerOptions, RssShuffleWriteManager, RssUtils, WriterAggregationManager, WriterBufferManager, WriterNoAggregationManager}
import org.apache.spark.shuffle.sort.RssUnsafeShuffleWriter

class RssShuffleWriter[K, V, C](
                                 user: String,
                                 rssServers: ServerList,
                                 writeClient: ShuffleDataWriter,
                                 mapInfo: AppTaskAttemptId,
                                 numMaps: Int,
                                 serializer: Serializer,
                                 bufferOptions: BufferManagerOptions,
                                 shuffleDependency: ShuffleDependency[K, V, C],
                                 stageMetrics: ShuffleClientStageMetrics,
                                 taskMetrics: TaskMetrics,
                                 conf: SparkConf,
                                 context: TaskContext) extends ShuffleWriter[K, V] with Logging {

  logInfo(s"Using ShuffleWriter: ${this.getClass.getSimpleName}, map task: $mapInfo, buffer: $bufferOptions")

  private val partitioner = shuffleDependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shouldPartition = numPartitions > 1
  private val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
  private var mapStatus: MapStatus = null

  private val ReductionFactorMetricName = "reductionFactor"
  private val SpillCountMetricName = "spillCount"

  val enableMapSideAggregation = shuffleDependency.mapSideCombine && conf.get(RssOpts.enableMapSideAggregation)
  val useUnsafeShuffleWriter = conf.get(RssOpts.useUnsafeShuffleWriter)

  val env = SparkEnv.get
  private val writerManager: RssShuffleWriteManager[K, V, C] = if (enableMapSideAggregation) {
    new WriterAggregationManager[K, V, C](writeClient, shuffleDependency, serializer, bufferOptions, conf)
  } else if (shuffleDependency.mapSideCombine) {
    new WriterNoAggregationManager(shuffleDependency, serializer, bufferOptions, writeClient, conf, taskMetrics)
  } else if (useUnsafeShuffleWriter) {
    new RssUnsafeShuffleWriter[K, V, C](writeClient, env.blockManager, context.taskMemoryManager(),
      shuffleDependency, context, conf)
  } else {
    new WriterBufferManager[K, V, C](writeClient, conf, shuffleDependency.partitioner.numPartitions, serializer, bufferOptions, taskMetrics)
  }

  logInfo(s"Using ${writerManager.getClass} as the shuffle writer manager.")

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    logInfo(s"Started processing records in Shuffle Map Task ($mapInfo), " +
      s"map side combine: ${shuffleDependency.mapSideCombine}")

    var numRecords = 0
    var recordFetchTime = 0L

    val startUploadStartTime = System.nanoTime()
    writeClient.startUpload(mapInfo, numMaps, numPartitions)
    val startUploadTime = System.nanoTime() - startUploadStartTime

    var recordFetchStartTime = System.nanoTime()
    while (records.hasNext) {
      numRecords = numRecords + 1
      val record = records.next()
      recordFetchTime += (System.nanoTime() - recordFetchStartTime)
      val partition = getPartition(record._1)
      writerManager.addRecord(partition, record)
      recordFetchStartTime = System.nanoTime()
    }
    writerManager.clear()

    val finishUploadStartTime = System.nanoTime()
    writeClient.finishUpload()
    val finishUploadTime = System.nanoTime() - finishUploadStartTime

    val totalBytes = writeClient.getShuffleWriteBytes()
    val shuffleWriteTimeMetadata = writerManager.getShuffleWriteTimeMetadata
    val shuffleWriteMetadata = writerManager.getShuffleWriteMetadata
    val writeTime = startUploadTime + shuffleWriteTimeMetadata.uploadTime + finishUploadTime

    implicit def bool2Long(bool: Boolean): Long = if (bool) 1L else 0L

    val supplWriteMetrics: List[(String, Long)] = List(
      ("shuffleWriteMapSideCombine", shuffleDependency.mapSideCombine: Long),
      ("shuffleWriteSpillCount", shuffleWriteMetadata.numberOfSpills),
      ("shuffleWriteRecordsRead", numRecords),
      ("shuffleWriteRecordsWritten", writerManager.recordsWritten),
      ("shuffleWriteBytesWritten", totalBytes),
      ("shuffleWriteWriteTimeNanoSec", writeTime),
      ("shuffleWriteSerializationTime", shuffleWriteTimeMetadata.serializationTime),
      ("shuffleWriteCompressionTime", shuffleWriteTimeMetadata.compressionTime),
      ("shuffleWriteUploadTime", shuffleWriteTimeMetadata.uploadTime),
      ("shuffleWriteMemoryFetchTime", shuffleWriteTimeMetadata.memoryFetchTime),
      ("shuffleWriteSendDataBlockTime", shuffleWriteTimeMetadata.sendDataBlockTime),
      ("shuffleWriteReductionFactorPercent", (writerManager.reductionFactor * 100).toLong))

    val writeMetrics = supplWriteMetrics ++ List(
      ("mapSideCombine", shuffleDependency.mapSideCombine.toString),
      ("aggManager", writerManager.getClass.toString))

    M3Stats.getDefaultScope.gauge(ReductionFactorMetricName).update(writerManager.reductionFactor)
    M3Stats.getDefaultScope.gauge(SpillCountMetricName).update(shuffleWriteMetadata.numberOfSpills)

    logInfo(s"Wrote shuffle records ($mapInfo), " +
      s"$numRecords records read, ${writerManager.recordsWritten} records written, $totalBytes bytes, " +
      s"records fetch wait time: ${TimeUnit.NANOSECONDS.toMillis(recordFetchTime)} " +
      s"start upload time: ${TimeUnit.NANOSECONDS.toMillis(startUploadTime)} " +
      s"serialization time: ${TimeUnit.NANOSECONDS.toMillis(shuffleWriteTimeMetadata.serializationTime)} " +
      s"compression time: ${TimeUnit.NANOSECONDS.toMillis(shuffleWriteTimeMetadata.compressionTime)} " +
      s"memory fetch wait time: ${TimeUnit.NANOSECONDS.toMillis(shuffleWriteTimeMetadata.memoryFetchTime)} " +
      s"upload time: ${TimeUnit.NANOSECONDS.toMillis(shuffleWriteTimeMetadata.uploadTime)} " +
      s"send block time: ${TimeUnit.NANOSECONDS.toMillis(shuffleWriteTimeMetadata.sendDataBlockTime)} " +
      s"finish ack time: ${TimeUnit.NANOSECONDS.toMillis(finishUploadTime)} " +
      s"write metadata: ${writeMetrics.toString()}")

    shuffleWriteMetrics.incRecordsWritten(writerManager.recordsWritten)
    shuffleWriteMetrics.incBytesWritten(totalBytes)
    shuffleWriteMetrics.incWriteTime(writeTime)

    // fill non-zero length
    val nonZeroPartitionLengths = shuffleWriteMetadata.partitionLengths.map(x => if (x == 0) 1 else x)

    val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, rssServers)
    mapStatus = MapStatus(blockManagerId, nonZeroPartitionLengths)

    writerManager.closeWriteClientAsync()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo(s"Stopped shuffle writer ($mapInfo), success: $success")

    writerManager.stop()
    writerManager.closeWriteClientAsync()

    if (success) {
      val remainingBytes = writerManager.collectionSizeInBytes
      if (remainingBytes != 0) {
        throw new RssInvalidStateException(s"Writer buffer should be empty, but still has $remainingBytes bytes, $mapInfo")
      }
      Option(mapStatus)
    } else {
      None
    }
  }
}