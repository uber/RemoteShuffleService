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

import com.esotericsoftware.kryo.io.Output
import com.uber.rss.exceptions.{RssInvalidDataException, RssMapperDataMismatchException}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.shuffle.RssOpts

import scala.collection.mutable
import scala.collection.mutable.Map

case class BufferManagerOptions(individualBufferSize: Int, individualBufferMax: Int, bufferSpillThreshold: Int)

case class WriterBufferManagerValue(serializeStream: SerializationStream, output: Output)

class WriterBufferManager[K, V, C](shuffleWriteCurator: ShuffleWriteCurator,
                                   serializer: Serializer,
                                   bufferSize: Int,
                                   maxBufferSize: Int,
                                   spillSize: Int,
                                   confOpt: Option[SparkConf]=None)
  extends RssShuffleWriteManager[K, V, C](shuffleWriteCurator) {

  def this(shuffleWriteCurator: ShuffleWriteCurator, serializer: Serializer,
           bufferOptions: BufferManagerOptions, confOpt: Option[SparkConf]) {
    this(shuffleWriteCurator, serializer, bufferOptions.individualBufferSize, bufferOptions.individualBufferMax,
      bufferOptions.bufferSpillThreshold, confOpt)
  }

  def this(shuffleWriteCurator: ShuffleWriteCurator, serializer: Serializer, bufferOptions: BufferManagerOptions) {
    this(shuffleWriteCurator, serializer, bufferOptions.individualBufferSize, bufferOptions.individualBufferMax,
      bufferOptions.bufferSpillThreshold)
  }

  private var recordsReadCount: Int = 0
  private var recordsWrittenCount: Int = 0
  private var totalSerializationTime: Long = 0l
  private var totalMemoryFetchWaitTime: Long = 0l
  // Total size of serialized data stored in the map
  private var totalBytes = 0

  private val serializerInstance = serializer.newInstance()
  private val map: Map[Int, WriterBufferManagerValue] = Map()
  private val partitionIdsToSpill: mutable.Set[Int] = mutable.Set[Int]()
  private val enableRecordCountMatchCheck =
    confOpt.map(conf => conf.get(RssOpts.enableBufferMapperRecordCountCheck)).getOrElse(false)

  override def addRecord(partitionId: Int, record: Product2[K, V]): Unit = {
    recordsReadCount += 1
    addRecordImpl(partitionId, record)
  }

  private[rss] def addRecordImpl(partitionId: Int, record: Product2[Any, Any]): Unit = {
    val serStartTime = System.nanoTime()
    val shuffleDataForPartition = map.get(partitionId)
    shuffleDataForPartition match {
      case Some(v) =>
        val stream = v.serializeStream
        val oldSize = v.output.position()
        stream.writeKey(record._1)
        stream.writeValue(record._2)
        recordsWrittenCount += 1
        val newSize = v.output.position()
        totalSerializationTime += System.nanoTime() - serStartTime
        if (newSize >= bufferSize) {
          // partition buffer is full, add it to the partition to be spilled
          partitionIdsToSpill.add(partitionId)
          totalBytes -= oldSize
        } else {
          totalBytes += (newSize - oldSize)
        }
      case None =>
        val output = new Output(bufferSize, maxBufferSize)
        val stream = serializerInstance.serializeStream(output)
        stream.writeKey(record._1)
        stream.writeValue(record._2)
        val newSize = output.position()
        if (newSize >= bufferSize) {
          totalSerializationTime += System.nanoTime() - serStartTime
          // Size of single record exceeds the spill threshold. Directly send this record to server
          // to avoid copying to the map
          stream.flush()
          shuffleWriteCurator.sendDataBlocks(singlePartitionCollectionIterator(partitionId, output.toBytes))
          stream.close()
          recordsWrittenCount += 1
          return
        } else {
          map.put(partitionId, WriterBufferManagerValue(stream, output))
          recordsWrittenCount += 1
          totalBytes = totalBytes + newSize
        }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all partitions to be spilled to the result as spill data
      partitionIdsToSpill ++= map.keys
      totalBytes = 0
    }

    if (partitionIdsToSpill.nonEmpty) {
      shuffleWriteCurator.sendDataBlocks(
        multiPartitionCollectionIterator(map, partitionIdsToSpill.toSeq))
      partitionIdsToSpill.clear()
    }
  }

  /**
   * Returns an iterator over single serialized record of give partition ID
   */
  def singlePartitionCollectionIterator(partitionId: Int,
                                        data: Array[Byte]): Iterator[(Int, Array[Byte], Int)] = {
    new Iterator[(Int, Array[Byte], Int)] {

      private var isLastRecord = false

      override def hasNext: Boolean = !isLastRecord

      override def next(): (Int, Array[Byte], Int) = {
        isLastRecord = true
        val fetchStartTime = System.nanoTime()
        val output = (partitionId, data, -1)
        totalMemoryFetchWaitTime += System.nanoTime() - fetchStartTime
        output
      }
    }
  }

  def multiPartitionCollectionIterator(mapRef: Map[Int, WriterBufferManagerValue],
                                       partitionIdsToSpill: Seq[Int]): Iterator[(Int, Array[Byte], Int)] = {
    new Iterator[(Int, Array[Byte], Int)] {

      var currentPartitionIndex = 0

      override def hasNext: Boolean = currentPartitionIndex < partitionIdsToSpill.size

      override def next(): (Int, Array[Byte], Int) = {
        val memoryFetchStartTime = System.nanoTime()

        if (!hasNext) {
          throw new NoSuchElementException
        }
        val partitionId = partitionIdsToSpill(currentPartitionIndex)
        mapRef(partitionId).serializeStream.flush()
        val elem = (partitionId, mapRef(partitionId).output.toBytes, -1)
        mapRef(partitionId).serializeStream.close()
        mapRef.remove(partitionId)
        currentPartitionIndex += 1
        totalMemoryFetchWaitTime += System.nanoTime() - memoryFetchStartTime
        elem
      }
    }
  }

  override def collectionSizeInBytes: Int = {
    map.values.foreach(t => flushStream(t.serializeStream, t.output))
    val sum = map.map(_._2.output.position()).sum
    if (sum != totalBytes) {
      throw new RssInvalidDataException(s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  override def finish(): Unit = {
    if (!map.isEmpty) {
      totalBytes = 0
      shuffleWriteCurator.sendDataBlocks(
        multiPartitionCollectionIterator(map, map.keys.toSeq))
    }
    if (recordsReadCount != recordsWrittenCount && enableRecordCountMatchCheck) {
      throw new RssMapperDataMismatchException(recordsReadCount, recordsWrittenCount, this.getClass.getSimpleName)
    }
  }

  private def flushStream(serializeStream: SerializationStream, output: Output) = {
    val oldPosition = output.position()
    serializeStream.flush()
    val numBytes = output.position() - oldPosition
    totalBytes += numBytes
  }

  override def getShuffleWriteTimeMetadata(): ShuffleWriteTimeMetadata = {
    ShuffleWriteTimeMetadata(totalSerializationTime, shuffleWriteCurator.totalCompressionTime,
      shuffleWriteCurator.totalUploadTime, totalMemoryFetchWaitTime)
  }

  override def getShuffleWriteMetadata(): ShuffleWriteMetadata = {
    ShuffleWriteMetadata(recordsReadCount, recordsWrittenCount, shuffleWriteCurator.numberOfSpills,
      shuffleWriteCurator.partitionLengths, 0.0, shuffleWriteCurator.numberOfNetworkWrites)
  }
}
