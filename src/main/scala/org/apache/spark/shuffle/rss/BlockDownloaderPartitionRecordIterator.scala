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

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.uber.rss.clients.{RecordKeyValuePair, RecordReader}
import com.uber.rss.metrics.M3Stats
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance

class BlockDownloaderPartitionRecordIterator[K, C](
    shuffleId: Int,
    partition: Int,
    serializer: SerializerInstance,
    downloader: RecordReader,
    shuffleReadMetrics: ShuffleReadMetrics) extends Iterator[Product2[K, C]] with Logging {

  private var nextRecord: RecordKeyValuePair = null

  private var numRecords = 0L

  readRecordFromDownloader()

  // time when the first record is fetched
  private var fetchStartNanoTime = 0L

  // how much time spent on network (download data from shuffle server)
  private var fetchNanoTime = 0L

  // how much time spent on deserialize
  private var deserializeTime = 0L

  private var lastLogMillis = System.currentTimeMillis()
  private val logIntervalMillis = 30000L

  override def hasNext: Boolean = {
    nextRecord != null
  }

  override def next(): Product2[K, C] = {
    try {
      nextImpl()
    } catch {
      case e: Throwable =>
        M3Stats.addException(e, this.getClass().getSimpleName())
        throw e
    }
  }
  
  private def readRecordFromDownloader(): Unit = {
    val readRecordStartNanoTime = System.nanoTime()

    nextRecord = downloader.readRecord()

    fetchNanoTime += System.nanoTime() - readRecordStartNanoTime

    if (nextRecord == null) {
      downloader.close()
      shuffleReadMetrics.incRecordsRead(numRecords)
      shuffleReadMetrics.incRemoteBytesRead(downloader.getShuffleReadBytes())
      shuffleReadMetrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(fetchNanoTime))
      logShuffleFetchInfo(true)
    } else {
      numRecords = numRecords + 1
      logShuffleFetchInfo(false)
    }
  }
  
  private def nextImpl(): Product2[K, C] = {
    if (fetchStartNanoTime == 0) {
      fetchStartNanoTime = System.nanoTime()
    }

    val deserializeStartTime = System.nanoTime()

    val key: K = if (nextRecord.getKey() == null) {
      null.asInstanceOf[K]
    } else {
      serializer.deserialize(ByteBuffer.wrap(nextRecord.getKey()))
    }

    val value = if (nextRecord.getValue() == null) {
      null.asInstanceOf[C]
    } else {
      serializer.deserialize(ByteBuffer.wrap(nextRecord.getValue()))
    }

    deserializeTime += System.nanoTime() - deserializeStartTime

    val result = (key, value)

    readRecordFromDownloader()

    result
  }

  private def logShuffleFetchInfo(downloadFinished: Boolean): Unit = {
    val currentMillis = System.currentTimeMillis()
    if (downloadFinished || currentMillis - lastLogMillis > logIntervalMillis) {
      val durationMillisLogString = if (fetchStartNanoTime == 0) {
        "(fetch not started)"
      } else {
        String.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fetchStartNanoTime))
      }
      val downloadMillis = TimeUnit.NANOSECONDS.toMillis(fetchNanoTime)
      val deserializeMillis = TimeUnit.NANOSECONDS.toMillis(deserializeTime)
      logInfo(s"ShuffleFetchInfo: shuffle $shuffleId partition $partition, $numRecords records, ${downloader.getShuffleReadBytes()} bytes, duration millis: $durationMillisLogString, fetch millis: $downloadMillis, deserialize millis: $deserializeMillis, finished: $downloadFinished")
      lastLogMillis = currentMillis
    }
  }
}
