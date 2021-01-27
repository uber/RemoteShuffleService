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

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.RssOpts

//ToDo: Fix/Check number of bytes written
/**
 * Does a best effort map side aggregation. Even if the partial aggregation is not complete, reducer side combiners
 * will take of aggregating the partially aggregated results within the mapper partition.
 * [[org.apache.spark.util.collection.PartitionedAppendOnlyMap]]'s
 * map implementation is used since it provides a functionality to estimate the size of the map.
 * To make the keys compatible with PartitionedAppendOnlyMap, each key is of the format (partitionId, aggKey).
 *
 * The map can grow up to the size of up to [[RssOpts.rssMapSideAggInitialMemoryThreshold]], post that twice the memory
 * deficit is requested from the task memory manager everytime the allocated quota is exhausted. If the memory is
 * not allocated the data is spilled (sent to RSS servers). Acquiring memory dynamically comes with a caveat that,
 * the allocated memory can be requested by the task memory manager by calling
 * [[org.apache.spark.memory.MemoryConsumer#spill]] implementation.
 * In such case we set the [[org.apache.spark.shuffle.rss.WriterAggregationManager#forceSpill()]]
 * flag to false, so that whenever next record is written to map, entire map is spilled.
 */
class WriterAggregationManager[K, V, C](taskMemoryManager: TaskMemoryManager,
                                        shuffleDependency: ShuffleDependency[K, V, C],
                                        serializer: Serializer,
                                        bufferOptions: BufferManagerOptions,
                                        conf: SparkConf)
  extends WriteBufferManager[K, V, C](serializer, bufferOptions) with Logging {

  private val initialMemoryThreshold: Long = conf.get(RssOpts.rssMapSideAggInitialMemoryThreshold)

  private val aggImpl = new WriterAggregationImpl(taskMemoryManager,
    shuffleDependency, serializer, bufferOptions, conf)

  override def recordsWritten: Int = aggImpl.recordsWritten

  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    aggImpl.addRecord(partitionId, record)
  }

  override def clear(): Seq[(Int, Array[Byte])] = {
    aggImpl.spillMap()
  }

  override def filledBytes: Int = {
    aggImpl.filledBytes
  }

  override def releaseMemory(memoryToHold: Long = initialMemoryThreshold): Unit = {
    aggImpl.releaseMemory(memoryToHold)
  }
}
