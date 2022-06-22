/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import java.io.ByteArrayOutputStream

import com.uber.rss.exceptions.RssInvalidDataException

import scala.collection.mutable
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}

case class BufferManagerOptions(individualBufferSize: Int, individualBufferMax: Int,
                                bufferSpillThreshold: Int, supportAggregate: Boolean)

case class PartitionRecordBuffer(serializeStream: SerializationStream,
                                 output: ByteArrayOutputStream)

class DefaultRecordBufferManager[K, V](serializer: Serializer,
                                       bufferSize: Int,
                                       spillSize: Int,
                                       numPartitions: Int,
                                       createCombiner: Option[V => Any] = None)
    extends RecordBufferManager[K, V]
    with Logging {
  private val partitionBuffers: Array[PartitionRecordBuffer] =
    new Array[PartitionRecordBuffer](numPartitions)

  private var totalBytes = 0

  private val serializerInstance = serializer.newInstance()

  def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte], Int)] = {
    val key: Any = record._1
    val value: Any = createCombiner.map(_.apply(record._2)).getOrElse(record._2)
    var result: mutable.Buffer[(Int, Array[Byte], Int)] = null
    val v = partitionBuffers(partitionId)
    if (v != null) {
      val stream = v.serializeStream
      val oldSize = v.output.size()
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = v.output.size()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte], Int)]()
        }
        v.serializeStream.flush()
        val bytes = v.output.toByteArray
        result.append((partitionId, bytes, bytes.length))
        v.serializeStream.close()
        partitionBuffers(partitionId) = null
        totalBytes -= oldSize
      } else {
        totalBytes += (newSize - oldSize)
      }
    }
    else {
      val output = new ByteArrayOutputStream(bufferSize)
      val stream = serializerInstance.serializeStream(output)
      stream.writeKey(key)
      stream.writeValue(value)
      val newSize = output.size()
      if (newSize >= bufferSize) {
        // partition buffer is full, add it to the result as spill data
        if (result == null) {
          result = mutable.Buffer[(Int, Array[Byte], Int)]()
        }
        stream.flush()
        val bytes = output.toByteArray
        result.append((partitionId, bytes, bytes.length))
        stream.close()
      } else {
        partitionBuffers(partitionId) = PartitionRecordBuffer(stream, output)
        totalBytes = totalBytes + newSize
      }
    }

    if (totalBytes >= spillSize) {
      // data for all partitions exceeds threshold, add all data to the result as spill data
      if (result == null) {
        result = mutable.Buffer[(Int, Array[Byte], Int)]()
      }
      val allData = clear()
      result.appendAll(allData)
    }

    if (result == null) {
      Nil
    } else {
      result
    }
  }

  def filledBytes: Int = {
    var i = 0
    var sum = 0
    while (i < partitionBuffers.length) {
      val t = partitionBuffers(i)
      if (t != null) {
        flushStream(t.serializeStream, t.output)
        sum += t.output.size()
      }
      i += 1
    }
    if (sum != totalBytes) {
      throw new RssInvalidDataException(
        s"Inconsistent internal state, total bytes is $totalBytes, but should be $sum")
    }
    totalBytes
  }

  def clear(): Seq[(Int, Array[Byte], Int)] = {
    val result = mutable.Buffer[(Int, Array[Byte], Int)]()
    var i = 0
    while (i < partitionBuffers.length) {
      val t = partitionBuffers(i)
      if (t != null) {
        t.serializeStream.flush()
        val bytes = t.output.toByteArray
        result.append((i, bytes, bytes.length))
        t.serializeStream.close()
        partitionBuffers(i) = null
      }
      i += 1
    }
    totalBytes = 0
    result
  }

  private def flushStream(serializeStream: SerializationStream, output: ByteArrayOutputStream) = {
    val oldPosition = output.size()
    serializeStream.flush()
    val numBytes = output.size() - oldPosition
    totalBytes += numBytes
  }
}
