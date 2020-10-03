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
import com.uber.rss.common.{AppTaskAttemptId, ServerList}
import com.uber.rss.exceptions.RssInvalidStateException
import com.uber.rss.metrics.ShuffleClientStageMetrics
import io.netty.buffer.Unpooled
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory}
import org.apache.spark.ShuffleDependency
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.rss.{BufferManagerOptions, RssUtils, WriteBufferManager}

class RssShuffleWriter[K, V, C](
                                 user: String,
                                 rssServers: ServerList,
                                 writeClient: RecordWriter,
                                 mapInfo: AppTaskAttemptId,
                                 serializer: Serializer,
                                 bufferOptions: BufferManagerOptions,
                                 shuffleDependency: ShuffleDependency[K, V, C],
                                 stageMetrics: ShuffleClientStageMetrics,
                                 shuffleWriteMetrics: ShuffleWriteMetrics)
    extends ShuffleWriter[K, V] with Logging {

  logInfo(s"Using ShuffleWriter: ${this.getClass.getSimpleName}, map task: $mapInfo, buffer: $bufferOptions")

  private val partitioner = shuffleDependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shouldPartition = numPartitions > 1

  private val writeClientCloseLock = new Object()

  private val bufferManager = new WriteBufferManager(
    serializer = serializer,
    bufferSize = bufferOptions.individualBufferSize,
    maxBufferSize = bufferOptions.individualBufferMax,
    spillSize = bufferOptions.bufferSpillThreshold)

  private val compressor = LZ4Factory.fastestInstance.fastCompressor

  private var mapStatus: MapStatus = null

  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.getPartition(key) else 0
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    logInfo(s"Writing shuffle records ($mapInfo), map side combine: ${shuffleDependency.mapSideCombine}")

    var numRecords = 0

    val startUploadStartTime = System.nanoTime()
    val numMaps = 1 // TODO hack for Spark 3.0, remove this later
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

      var spilledData: Seq[(Int, Array[Byte])] = null

      if (shuffleDependency.mapSideCombine) {
        val createCombiner = shuffleDependency.aggregator.get.createCombiner
        val c = createCombiner(record._2)
        val serializeStartTime = System.nanoTime()
        spilledData = bufferManager.addRecord(partition, (record._1, c))
        serializeTime += (System.nanoTime() - serializeStartTime)
      } else {
        val serializeStartTime = System.nanoTime()
        spilledData = bufferManager.addRecord(partition, record)
        serializeTime += (System.nanoTime() - serializeStartTime)
      }

      sendDataBlocks(spilledData)

      numRecords = numRecords + 1
      writeRecordTime += (System.nanoTime() - writeRecordStartTime)

      recordFetchStartTime = System.nanoTime()
    }

    val remainingData = bufferManager.clear()
    sendDataBlocks(remainingData)

    val finishUploadStartTime = System.nanoTime()
    writeClient.finishUpload()
    val finishUploadTime = System.nanoTime() - finishUploadStartTime

    val totalBytes = writeClient.getShuffleWriteBytes()
    logInfo(s"Wrote shuffle records ($mapInfo), $numRecords records, $totalBytes bytes, write seconds: ${TimeUnit.NANOSECONDS.toSeconds(startUploadTime)}, ${TimeUnit.NANOSECONDS.toSeconds(writeRecordTime)}, ${TimeUnit.NANOSECONDS.toSeconds(finishUploadTime)}, serialize seconds: ${TimeUnit.NANOSECONDS.toSeconds(serializeTime)}, record fetch seconds: ${TimeUnit.NANOSECONDS.toSeconds(recordFetchTime)}")

    shuffleWriteMetrics.incRecordsWritten(numRecords)
    shuffleWriteMetrics.incBytesWritten(totalBytes)
    shuffleWriteMetrics.incWriteTime(startUploadTime + writeRecordTime + finishUploadTime)

    closeWriteClientAsync()

    // fill partitionLengths with non zero dummy value so map output tracker could work correctly
    val partitionLengths: Array[Long] = Array.fill(numPartitions)(1L)
    val blockManagerId = RssUtils.createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId, rssServers)
    mapStatus = MapStatus(blockManagerId, partitionLengths, mapInfo.getMapId)
  }

  private def sendDataBlocks(fullFilledData: Seq[(Int, Array[Byte])]) = {
    fullFilledData.foreach(t => {
      val partitionId = t._1
      val bytes = t._2
      if (bytes != null && bytes.length > 0) {
        val dataBlock = createDataBlock(bytes)
        writeClient.sendRecord(partitionId, null, dataBlock)
      }
    })
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo(s"Stopped shuffle writer ($mapInfo), success: $success")

    closeWriteClientAsync()

    if (success) {
      val remainingBytes = bufferManager.filledBytes
      if (remainingBytes != 0) {
        throw new RssInvalidStateException(s"Writer buffer should be empty, but still has $remainingBytes bytes, $mapInfo")
      }
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
