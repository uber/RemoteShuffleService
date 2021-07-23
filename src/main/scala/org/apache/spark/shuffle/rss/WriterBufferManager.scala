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
import com.uber.rss.clients.ShuffleDataWriter
import com.uber.rss.exceptions.RssInvalidDataException
import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer, SerializerInstance}

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.Map

case class BufferManagerOptions(individualBufferSize: Int, individualBufferMax: Int, bufferSpillThreshold: Int)

case class WriterBufferManagerValue(serializeStream: SerializationStream, output: Output)

class WriterBufferManager [K, V, C](writeClient: ShuffleDataWriter,
                                    conf: SparkConf,
                                    numPartitions: Int,
                                    serializer: Serializer,
                                    bufferSize: Int,
                                    maxBufferSize: Int,
                                    spillSize: Int,
                                    taskMetrics: TaskMetrics)
  extends RssShuffleWriteManager[K, V, C](writeClient, conf, numPartitions) with Logging {

  def this(writeClient: ShuffleDataWriter, conf: SparkConf, numPartitions: Int, serializer: Serializer, bufferOptions: BufferManagerOptions, taskMetrics: TaskMetrics) {
    this(writeClient, conf, numPartitions, serializer, bufferOptions.individualBufferSize,
      bufferOptions.individualBufferMax, bufferOptions.bufferSpillThreshold, taskMetrics)
  }

  private val map: Map[Int, WriterBufferManagerValue] = Map()

  private var totalBytes = 0

  override def recordsWritten: Int = recordsWrittenCount

  override def reductionFactor: Double = 0.0

  private var recordsWrittenCount: Int = 0

  private var totalSerializationTime: Long = 0l

  private var totalMemoryFethcWaitTime: Long = 0l

  private val serializerInstance = serializer.newInstance()

  var totalLookUpTime = 0L

  override def addRecord(partitionId: Int, record: Product2[K, V]): Unit = {
    addRecordImpl(partitionId, record)
  }

  private[rss] def addRecordImpl(partitionId: Int, record: Product2[Any, Any]): Unit = {
    val serStartTime = System.nanoTime()
    var partitionIdsToSpill: mutable.ArrayBuffer[Int] = null
    recordsWrittenCount += 1
    val lookUpStartTime = System.nanoTime()
    val rr = map.get(partitionId)
    totalLookUpTime += System.nanoTime() - lookUpStartTime
    rr match {
      case Some(v) =>
        val stream = v.serializeStream
        val oldSize = v.output.position()
        stream.writeKey(record._1)
        stream.writeValue(record._2)
        val newSize = v.output.position()
        if (newSize >= bufferSize) {
          // partition buffer is full, add it to the partition to be spilled
          if (partitionIdsToSpill == null) {
            partitionIdsToSpill = mutable.ArrayBuffer[Int]()
          }
          partitionIdsToSpill.append(partitionId)
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
          totalSerializationTime += System.nanoTime() - serStartTime;
          return sendDataBlocks(new Iterator[(Int, Array[Byte], Int)] {
            var hasMore = true
            override def hasNext: Boolean = hasMore
            override def next(): (Int, Array[Byte], Int) = {
              hasMore = false
              val ss = System.nanoTime()
              val aa = (partitionId, output.toBytes, -1)
              totalMemoryFethcWaitTime += System.nanoTime() - ss
              aa
            }
          })
        } else {
          map.put(partitionId, WriterBufferManagerValue(stream, output))
          totalBytes = totalBytes + newSize
        }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all partitions to be spilled to the result as spill data
      if (partitionIdsToSpill == null) {
        partitionIdsToSpill = mutable.ArrayBuffer[Int]()
      }
      partitionIdsToSpill.appendAll(map.keys)
      totalBytes = 0
    }
    totalSerializationTime += System.nanoTime() - serStartTime;

    sendDataBlocks(if (partitionIdsToSpill != null) {
      getBufferIterator(map, partitionIdsToSpill.toSet.toSeq)
    } else {
      Iterator.empty
    })
  }

  def getBufferIterator(mapRef: Map[Int, WriterBufferManagerValue], partitionIdsToSpill: Seq[Int]): Iterator[(Int, Array[Byte], Int)] = {
    new Iterator[(Int, Array[Byte], Int)] {

      var currentPartitionIndex = 0

      override def hasNext: Boolean = currentPartitionIndex < partitionIdsToSpill.size

      override def next(): (Int, Array[Byte], Int) = {
        val ss = System.nanoTime()

        if (!hasNext) {
          throw new NoSuchElementException
        }
        val partitionId = partitionIdsToSpill(currentPartitionIndex)
        mapRef(partitionId).serializeStream.flush()
        val elem = (partitionId, mapRef(partitionId).output.toBytes, -1)
        mapRef(partitionId).serializeStream.close()
        mapRef.remove(partitionId)
        currentPartitionIndex += 1
        totalMemoryFethcWaitTime += System.nanoTime() - ss
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

  override def clear(): Unit = {
    totalBytes = 0
    if (!map.isEmpty) {
      sendDataBlocks(getBufferIterator(map, map.keys.toSeq))
    }
  }

  private def flushStream(serializeStream: SerializationStream, output: Output) = {
    val oldPosition = output.position()
    serializeStream.flush()
    val numBytes = output.position() - oldPosition
    totalBytes += numBytes
  }

  override def stop(): Unit = {}

  override def getShuffleWriteTimeMetadata(): ShuffleWriteTimeMetadata = {
    ShuffleWriteTimeMetadata(totalSerializationTime, totalCompressionTime, totalUploadTime, totalMemoryFethcWaitTime)
  }

  override def getShuffleWriteMetadata(): ShuffleWriteMetadata = {
    ShuffleWriteMetadata(recordsRead = recordsWritten, recordsWritten = recordsWritten, bytesWritten = 0l, numberOfSpills = getNumOfSpills(), partitionLengths = partitionLengths)
  }

}
