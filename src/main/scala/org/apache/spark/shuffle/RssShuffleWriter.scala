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
import com.uber.rss.clients.ShuffleDataWriter
import com.uber.rss.common.{AppTaskAttemptId, ServerList}
import com.uber.rss.exceptions.RssInvalidStateException
import com.uber.rss.metrics.ShuffleClientStageMetrics
import net.jpountz.lz4.LZ4Factory
import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.executor.{AdditionalTaskMetrics, ShuffleWriteMetrics, SupplementaryMetric, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.rss.{BufferManagerOptions, RssUtils, WriteBufferManager, WriterAggregationManager, WriterAggregationMapper}

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
    taskMemoryManager: TaskMemoryManager,
    conf: SparkConf) extends ShuffleWriter[K, V] with Logging {

  logInfo(s"Using ShuffleWriter: ${this.getClass.getSimpleName}, map task: $mapInfo, buffer: $bufferOptions")

  private val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
  private val partitioner = shuffleDependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shouldPartition = numPartitions > 1

  private val writeClientCloseLock = new Object()
  private var mapStatus: MapStatus = null

  val enableMapSideAggregation = shuffleDependency.mapSideCombine && conf.get(RssOpts.enableMapSideAggregation)

  private val writerManager: WriteBufferManager[K, V, C] = if (enableMapSideAggregation) {
    new WriterAggregationManager[K, V, C](taskMemoryManager, shuffleDependency, serializer, bufferOptions, conf)
  } else if (shuffleDependency.mapSideCombine) {
    new WriterAggregationMapper(shuffleDependency, serializer, bufferOptions)
  } else{
    new WriteBufferManager[K, V, C](
      serializer = serializer,
      bufferSize = bufferOptions.individualBufferSize,
      maxBufferSize = bufferOptions.individualBufferMax,
      spillSize = bufferOptions.bufferSpillThreshold)
  }

  private val compressor = LZ4Factory.fastestInstance.fastCompressor

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

    val partitionLengths: Array[Long] = Array.fill[Long](numPartitions)(0L)

    while (records.hasNext) {
      val record = records.next()
      recordFetchTime += (System.nanoTime() - recordFetchStartTime)

      val writeRecordStartTime = System.nanoTime()

      val partition = getPartition(record._1)

      var spilledData: Seq[(Int, Array[Byte])] = null

      val serializeStartTime = System.nanoTime()
      spilledData = writerManager.addRecord(partition, record)
      serializeTime += (System.nanoTime() - serializeStartTime)
      if (!spilledData.isEmpty) {
        sendDataBlocks(spilledData, partitionLengths)
        writerManager.releaseMemory()
      }
      numRecords = numRecords + 1
      writeRecordTime += (System.nanoTime() - writeRecordStartTime)
      recordFetchStartTime = System.nanoTime()
    }

    val writeRecordStartTime = System.nanoTime()
    val serializeStartTime = System.nanoTime()
    val remainingData = writerManager.clear()
    serializeTime += (System.nanoTime() - serializeStartTime)
    if (!remainingData.isEmpty) {
      //ToDo: These blocks will not be of the protobuf recommended size of 32kb. Should we fix this?
      sendDataBlocks(remainingData, partitionLengths)
      writerManager.releaseMemory()
    }
    writeRecordTime += (System.nanoTime() - writeRecordStartTime)

    numRecords = writerManager.recordsWritten

    val finishUploadStartTime = System.nanoTime()
    writeClient.finishUpload()
    val finishUploadTime = System.nanoTime() - finishUploadStartTime

    val totalBytes = writeClient.getShuffleWriteBytes()

    val writeMetrics = List(("mapSideCombine", shuffleDependency.mapSideCombine.toString),
      ("reductionFactor", writerManager.reductionFactor.toString),
      ("spillCount", writerManager.numberOfSpills.toString),
      ("aggManager", writerManager.getClass.toString),
      ("recordsWritten", writerManager.recordsWritten.toString))

    logInfo(s"Wrote shuffle records ($mapInfo), " +
      s"$numRecords records, $totalBytes bytes, " +
      s"write seconds: ${TimeUnit.NANOSECONDS.toSeconds(startUploadTime)}, " +
      s"${TimeUnit.NANOSECONDS.toSeconds(writeRecordTime)}, " +
      s"${TimeUnit.NANOSECONDS.toSeconds(finishUploadTime)}, " +
      s"serialize seconds: ${TimeUnit.NANOSECONDS.toSeconds(serializeTime)}, " +
      s"record fetch seconds: ${TimeUnit.NANOSECONDS.toSeconds(recordFetchTime)}," +
      s"write metadata: ${writeMetrics.toString()}")

    shuffleWriteMetrics.incRecordsWritten(numRecords)
    shuffleWriteMetrics.incBytesWritten(totalBytes)
    shuffleWriteMetrics.incWriteTime(startUploadTime + writeRecordTime + finishUploadTime)

    // fill non-zero length
    val nonZeroPartitionLengths = partitionLengths.map(x => if (x == 0) 1 else x)

    val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, rssServers)
    mapStatus = MapStatus(blockManagerId, nonZeroPartitionLengths)

    closeWriteClientAsync()
  }

  private def sendDataBlocks(fullFilledData: Seq[(Int, Array[Byte])], partitionLengths: Array[Long]) = {
    fullFilledData.foreach(t => {
      val partitionId = t._1
      val bytes = t._2
      if (bytes != null && bytes.length > 0) {
        val dataBlock = createDataBlock(bytes)
        writeClient.writeDataBlock(partitionId, dataBlock)

        partitionLengths(partitionId) += bytes.length
      }
    })
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo(s"Stopped shuffle writer ($mapInfo), success: $success")

    closeWriteClientAsync()

    if (success) {
      val remainingBytes = writerManager.filledBytes
      if (remainingBytes != 0) {
        throw new RssInvalidStateException(s"Writer buffer should be empty, but still has $remainingBytes bytes, $mapInfo")
      }
      writerManager.releaseMemory(0)
      Option(mapStatus)
    } else {
      None
    }
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

  private def createDataBlock(buffer: Array[Byte]): ByteBuffer = {
    val uncompressedByteCount = buffer.size
    val compressedBuffer = new Array[Byte](compressor.maxCompressedLength(uncompressedByteCount))
    val compressedByteCount = compressor.compress(buffer, compressedBuffer)
    val dataBlockByteBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + compressedByteCount)
    dataBlockByteBuffer.putInt(compressedByteCount)
    dataBlockByteBuffer.putInt(uncompressedByteCount)
    dataBlockByteBuffer.put(compressedBuffer, 0, compressedByteCount)
    dataBlockByteBuffer.flip
    dataBlockByteBuffer
  }
}
