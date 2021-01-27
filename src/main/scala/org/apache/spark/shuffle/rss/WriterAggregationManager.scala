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
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.RssOpts
import org.apache.spark.util.collection.PartitionedAppendOnlyMap

import scala.collection.mutable

//ToDo: Fix/Check number of bytes written
/**
 * Does a best effort map side aggregation. Even if the partial aggregation is not complete, reducer side combiners
 * will take of aggregating the partially aggregated results within the mapper partition.
 * [[org.apache.spark.util.collection.PartitionedAppendOnlyMap]]'s
 * map implementation is used since it provides a functionality to estimate the size of the map.
 * To make the keys compatible with PartitionedAppendOnlyMap, each key is of the format (partitionId, aggKey).
 *
 * The map can grow up to the size of up to [[RssOpts.rssMapSideAggInitialMemoryThreshold]], Once the
 * allocated memory is exhausted, data is spilled (sent to RSS servers).
 */
class WriterAggregationManager[K, V, C](shuffleDependency: ShuffleDependency[K, V, C],
                                        serializer: Serializer,
                                        bufferOptions: BufferManagerOptions,
                                        conf: SparkConf)
  extends WriteBufferManager[K, V, C](serializer, bufferOptions) with Logging {

  var map = new PartitionedAppendOnlyMap[K, C]
  var recordsWrittenCnt: Int = 0
  override def recordsWritten: Int = recordsWrittenCnt

  var numberOfSpills: Int = 0

  private val mergeValue = shuffleDependency.aggregator.get.mergeValue
  private val createCombiner = shuffleDependency.aggregator.get.createCombiner
  private var kv: Product2[K, V] = null
  val update = (hadValue: Boolean, oldValue: C) => {
    if (hadValue) {
      mergeValue(oldValue, kv._2)
    } else {
      createCombiner(kv._2)
    }
  }

  //ToDo: Allocate memory dynamically
  private[this] val initialMemoryThreshold: Int = conf.get(RssOpts.rssMapSideAggInitialMemoryThreshold)

  private val serializerInstance = serializer.newInstance()

  private def changeValue(key: (Int, K), updateFunc: (Boolean, C) => C): C = {
    map.changeValue(key, updateFunc)
  }

  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    kv = record
    changeValue((partitionId, kv._1), update)
    maybeSpillCollection()
  }

  private def maybeSpillCollection(): Seq[(Int, Array[Byte])] = {
    val estimatedSize = map.estimateSize()
    val (spill, spilledData) = mayBeSpill(estimatedSize)
    if (spill) {
      spilledData
    } else {
      Seq.empty
    }
  }

  private def mayBeSpill(currentMemory: Long): (Boolean, Seq[(Int, Array[Byte])])  = {
    if (currentMemory > initialMemoryThreshold) {
      logDebug(s"Exhausted allocated $initialMemoryThreshold memory. Spilling $currentMemory to RSS servers")
      val result = spillMap()
      (true, result)
    } else {
      (false, Seq.empty)
    }
  }

  private def spillMap(): Seq[(Int, Array[Byte])] = {
    numberOfSpills += 1
    val result = mutable.Buffer[(Int, Array[Byte])]()
    val output = new Output(initialMemoryThreshold, bufferOptions.individualBufferMax)
    val stream = serializerInstance.serializeStream(output)

    val itr = map.iterator
    //ToDo: Skip map side aggregation based on the reduction factor
    while (itr.hasNext) {
      val item = itr.next()
      val (key, value): Product2[Any, Any] = (item._1._2, item._2)
      stream.writeKey(key)
      stream.writeValue(value)
      stream.flush()
      result.append((item._1._1, output.toBytes))
      output.clear()
      recordsWrittenCnt += 1
    }
    stream.close()
    map = new PartitionedAppendOnlyMap[K, C]
    result
  }

  override def clear(): Seq[(Int, Array[Byte])] = {
    val result = spillMap()
    result
  }

  override def filledBytes: Int = {
    if (map.isEmpty) {
      0
    } else {
      map.estimateSize().toInt
    }
  }
}
