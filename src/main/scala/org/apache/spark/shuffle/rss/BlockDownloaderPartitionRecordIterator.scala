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

package org.apache.spark.shuffle.rss

import java.util.concurrent.TimeUnit
import com.esotericsoftware.kryo.io.Input
import com.github.luben.zstd.Zstd
import com.uber.rss.clients.{ShuffleDataReader, TaskDataBlock}
import com.uber.rss.common.Compression
import com.uber.rss.exceptions.{RssInvalidDataException, RssInvalidStateException}
import com.uber.rss.metrics.M3Stats
import com.uber.rss.util.{ByteBufUtils, ExceptionUtils}
import net.jpountz.lz4.{LZ4Factory, LZ4FastDecompressor}
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.shuffle.FetchFailedException

class BlockDownloaderPartitionRecordIterator[K, C](
                                                    shuffleId: Int,
                                                    partition: Int,
                                                    serializer: Serializer,
                                                    decompression: String,
                                                    downloader: ShuffleDataReader,
                                                    shuffleReadMetrics: ShuffleReadMetrics) extends Iterator[Product2[K, C]] with Logging {

  private val lz4Decompressor: LZ4FastDecompressor = if (Compression.COMPRESSION_CODEC_ZSTD.equals(decompression)) {
    null
  } else {
    LZ4Factory.fastestInstance.fastDecompressor()
  }

  private var downloaderEof = false

  private var deserializationInput: Input = null
  private var deserializationStream: DeserializationStream = null
  private var deserializationIterator: Iterator[(Any, Any)] = null

  private var numRecords = 0L
  private var numRemoteBytesRead = 0L

  // time when the first record is fetched
  private var fetchStartNanoTime = 0L

  // how much time spent on network (download data from shuffle server)
  private var fetchNanoTime = 0L

  // how much time spent on executing code in this class
  private var executeNanoTime = 0L

  // how much time spent on decompression data
  private var decompressTime = 0L

  // how much time spent on deserialize
  private var deserializeTime = 0L

  private var lastLogMillis = System.currentTimeMillis()
  private val logIntervalMillis = 30000L

  private val serializerInstance = serializer.newInstance()

  override def hasNext: Boolean = {
    val methodStartTime = System.nanoTime()

    if (fetchStartNanoTime == 0) {
      fetchStartNanoTime = methodStartTime
    }

    val deserializeStartTime = System.nanoTime()
    while ((deserializationIterator == null || !deserializationIterator.hasNext) && !downloaderEof) {
      deserializeTime += System.nanoTime() - deserializeStartTime
      fetchNextDeserializationIterator()
    }

    val deserializeStartTime2 = System.nanoTime()
    val result = if (deserializationIterator == null) {
      false
    } else {
      deserializationIterator.hasNext
    }
    deserializeTime += System.nanoTime() - deserializeStartTime2

    if (!result) {
      shuffleReadMetrics.incRecordsRead(numRecords)
      shuffleReadMetrics.incRemoteBytesRead(numRemoteBytesRead)
      shuffleReadMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(fetchNanoTime))
      logShuffleFetchInfo(true)
    }

    executeNanoTime += (System.nanoTime() - methodStartTime)
    result
  }

  override def next(): Product2[K, C] = {
    val methodStartTime = System.nanoTime()

    if (fetchStartNanoTime == 0) {
      fetchStartNanoTime = methodStartTime
    }

    if (deserializationIterator == null) {
      throw new RssInvalidStateException(
        s"deserializationIterator is null, please check hasNext before trying to get next, shuffle $shuffleId partition $partition, " + String.valueOf(downloader))
    }

    val deserializeStartTime = System.nanoTime()
    val result = deserializationIterator.next().asInstanceOf[(K, C)]
    deserializeTime += System.nanoTime() - deserializeStartTime
    if (result != null) {
      numRecords = numRecords + 1
    }

    executeNanoTime += (System.nanoTime() - methodStartTime)
    result
  }

  private def fetchNextDeserializationIterator(): Unit = {
    clearDeserializationStream()

    val readRecordStartNanoTime = System.nanoTime()
    var dataBlock: TaskDataBlock = null;

    try {
      dataBlock = downloader.readDataBlock()
      fetchNanoTime += System.nanoTime() - readRecordStartNanoTime

      while (dataBlock != null &&
        (dataBlock.getPayload == null || dataBlock.getPayload.size == 0)) {
        val readRecordStartNanoTime = System.nanoTime()
        dataBlock = downloader.readDataBlock()
        fetchNanoTime += System.nanoTime() - readRecordStartNanoTime
      }
    } catch {
      case ex: Throwable => {
        downloader.close()
        M3Stats.addException(ex, this.getClass().getSimpleName())
        throw new FetchFailedException(
          RssUtils.createReduceTaskDummyBlockManagerId(shuffleId, partition),
          shuffleId,
          -1,
          partition,
          s"Failed to read data fro shuffle $shuffleId partition $partition due to ${ExceptionUtils.getSimpleMessage(ex)})",
          ex)
      }
    }

    numRemoteBytesRead = downloader.getShuffleReadBytes

    if (dataBlock == null) {
      downloaderEof = true
      downloader.close()
      deserializationIterator = null
      return
    }

    val decompressStartTime = System.nanoTime()
    val bytes = dataBlock.getPayload
    val compressedLen = ByteBufUtils.readInt(bytes, 0)
    val uncompressedLen = ByteBufUtils.readInt(bytes, Integer.BYTES)
    val uncompressedBytes = new Array[Byte](uncompressedLen)
    if (Compression.COMPRESSION_CODEC_ZSTD.equals(decompression)) {
      val n = Zstd.decompress(uncompressedBytes, bytes)
      if (Zstd.isError(n)) {
        throw new RssInvalidDataException(
          s"Data corrupted for shuffle $shuffleId partition $partition, failed to decompress zstd, decompress returned: $n, " + String.valueOf(downloader))
      }
    } else {
      val count = lz4Decompressor.decompress(bytes, Integer.BYTES + Integer.BYTES, uncompressedBytes, 0, uncompressedLen)
      if (count != compressedLen) {
        throw new RssInvalidDataException(
          s"Data corrupted for shuffle $shuffleId partition $partition, expected compressed length: $compressedLen, but it is: $count, " + String.valueOf(downloader))
      }
    }
    decompressTime += (System.nanoTime() - decompressStartTime)

    deserializationInput = new Input(uncompressedBytes, 0, uncompressedLen)
    deserializationStream = serializerInstance.deserializeStream(deserializationInput)
    deserializationIterator = deserializationStream.asKeyValueIterator

    logShuffleFetchInfo(false)
  }

  private def logShuffleFetchInfo(downloadFinished: Boolean): Unit = {
    val currentMillis = System.currentTimeMillis()
    if (downloadFinished || currentMillis - lastLogMillis > logIntervalMillis) {
      val durationMillisLogString = if (fetchStartNanoTime == 0) {
        "(fetch not started)"
      } else {
        String.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fetchStartNanoTime))
      }
      val executeMillis = TimeUnit.NANOSECONDS.toMillis(executeNanoTime)
      val fetchMillis = TimeUnit.NANOSECONDS.toMillis(fetchNanoTime)
      val decompressMillis = TimeUnit.NANOSECONDS.toMillis(decompressTime)
      val deserializeMillis = TimeUnit.NANOSECONDS.toMillis(deserializeTime)
      logInfo(s"ShuffleFetchInfo: shuffle $shuffleId partition $partition, $numRecords records, $numRemoteBytesRead bytes, duration millis: $durationMillisLogString, execute millis: $executeMillis, fetch millis: $fetchMillis, decompress millis: $decompressMillis, deserialize millis: $deserializeMillis, finished: $downloadFinished")
      lastLogMillis = currentMillis
    }
  }

  private def clearDeserializationStream() = {
    if (deserializationInput != null) {
      val remainingBytes = deserializationInput.available()
      if (remainingBytes != 0) {
        throw new RssInvalidDataException(s"Data corrupted for shuffle $shuffleId partition $partition, there are still $remainingBytes bytes to read")
      }
      deserializationInput.close()
    }
    if (deserializationStream != null) {
      deserializationStream.close()
    }
    deserializationInput = null
    deserializationStream = null
  }
}
