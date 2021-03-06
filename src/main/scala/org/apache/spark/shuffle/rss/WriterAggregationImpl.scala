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
import org.apache.spark.internal.Logging
import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.RssOpts
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

import scala.collection.mutable

/**
 * Does a best effort map side aggregation. Even if the partial aggregation is not complete, reducer side combiners
 * will take of aggregating the partially aggregated results within the mapper partition.
 * [[org.apache.spark.util.collection.PartitionedAppendOnlyMap]]'s
 * map implementation is used since it provides a functionality to estimate the size of the map.
 * To make the keys compatible with PartitionedAppendOnlyMap, each key is of the format (partitionId, aggKey).
 *
 * The map can grow up to the size of up to [[RssOpts.initialMemoryThresholdInBytes]], Once the
 * allocated memory is exhausted, data is spilled (sent to RSS servers).
 */
private[rss] class WriterAggregationImpl[K, V, C](shuffleDependency: ShuffleDependency[K, V, C],
                                                  serializer: Serializer,
                                                  bufferOptions: BufferManagerOptions,
                                                  conf: SparkConf) extends Logging {

  private var map = new PartitionedAppendOnlyMap[K, C]
  private var recordsWrittenCnt: Int = 0

  private val mergeValue = shuffleDependency.aggregator.get.mergeValue
  private val createCombiner = shuffleDependency.aggregator.get.createCombiner
  private var currentRecord: Product2[K, V] = null

  private def update(hadValue: Boolean, oldValue: C): C = {
    if (hadValue) {
      mergeValue(oldValue, currentRecord._2)
    } else {
      createCombiner(currentRecord._2)
    }
  }

  // Before sending the map output values from the `PartitionedAppendOnlyMap` to RSS servers,
  // they are buffered (see `spillMap.result[mutable.Buffer]`).
  // The size of this buffer is roughly 1/10th the estimated size of the PartitionedAppendOnlyMap
  // So to be on the safe side, we consume only half the threshold map size
  private val initialMemoryThreshold: Long = conf.get(RssOpts.initialMemoryThresholdInBytes) / 2
  private val serializerInstance = serializer.newInstance()

  private[rss] def numberOfRecordsInMap: Int = map.size
  private[rss] def recordsWritten: Int = recordsWrittenCnt

  private def changeValue(key: (Int, K), updateFunc: (Boolean, C) => C): C = map.changeValue(key, updateFunc)

  /**
   * Checks if the record with key, `(partitionId, aggregationKey)` is present in the map and if it does,
   * merge it with the existing value in the map for that key.
   */
  private[rss] def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    currentRecord = record
    changeValue((partitionId, currentRecord._1), update)
    spillIfRequired()
  }


  private def spillIfRequired(): Seq[(Int, Array[Byte])] = {
    val estimatedSize = map.estimateSize()
    if (estimatedSize >= initialMemoryThreshold) {
      spillMap()
    } else {
      Seq.empty
    }
  }

  /**
   * Each entry in the hashmap is of the format,
   * (partitionId, aggregationKey) -> value
   *
   * First, sort the hashmap by the partition ID,
   * (partitionId<1>, (aggregationKey<1>, value)), (partitionId<1>, (aggregationKey<2>, value)),
   * (partitionId<2>, (aggregationKey<1>, value)), .....
   *
   * Then, combine all the values of each partition, serialize them and return,
   * [(partition1, Byte Array of serialized records for partition 1),
   *  (partition2, Byte Array of serialized records for partition 2),
   *  .....]
   */
  private[rss] def spillMap(): Seq[(Int, Array[Byte])] = {
    val result = mutable.Buffer[(Int, Array[Byte])]()
    val output = new Output(initialMemoryThreshold.toInt, bufferOptions.individualBufferMax)
    val stream = serializerInstance.serializeStream(output)
    try {
      // Sort the data only by the partition ID
      val partitionItr = getGroupedPartitionIterator(map.partitionedDestructiveSortedIterator(None))
      // Iterate over partitions
      while (partitionItr.hasNext) {
        val nxt = partitionItr.next()
        val partition = nxt._1
        // Iterate over all values for a given partition
        val recordItr = nxt._2
        while (recordItr.hasNext) {
          val item = recordItr.next()
          val (key, value): Product2[Any, Any] = (item._1, item._2)
          stream.writeKey(key)
          stream.writeValue(value)
          recordsWrittenCnt += 1
        }
        stream.flush()
        result.append((partition, output.toBytes))
        output.clear()
      }
    } catch {
      case e: Throwable =>
        logError(s"Error while spilling the hashmap: ${e.getMessage}")
        result.clear()
        throw e
    } finally {
      stream.close()
      map = new PartitionedAppendOnlyMap[K, C]
    }
    result
  }

  private def getGroupedPartitionIterator(data: Iterator[((Int, K), C)]) : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val buffered = data.buffered
    val numPartitions = shuffleDependency.partitioner.numPartitions
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private[rss] def collectionSizeInBytes: Int = {
    if (map.isEmpty) {
      0
    } else {
      map.estimateSize().toInt
    }
  }
}