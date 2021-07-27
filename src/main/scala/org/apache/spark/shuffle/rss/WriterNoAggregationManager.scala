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

import com.uber.rss.clients.ShuffleDataWriter
import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

/**
 * For aggregation, using RDD APIs user can provide three functions -
 * `creteCombiner` -> function to create an initial value for aggregation
 *    (e.g. create an array list with just one value to start with)
 * `mergeValue` -> function to merge new value into the prior aggregation result
 *    (e.g. add value to the end of the array list if the key matches)
 * `mergeCombiners` -> function to merge outputs of multiple mergeValue function
 *    (e.g. combine two array list with matching keys)
 *
 * For a simple `reduceByKey(_ + _)` operation,
 *  createCombiner would be,
 *      def creteCombiner(value: V): V = value
 *  mergeValue and mergeCombiner would be,
 *      def mergeValue/mergeCombiners(oldValue: V, newValue: V): V = oldValue + newValue
 *
 * Only applies combiner function to each record and does not aggregate records. Reducer will aggregate
 * these records while reading, [[org.apache.spark.shuffle.RssShuffleReader#read()]]
 * This is logically equivalent of map side aggregation with reduction factor as 0.
 */
class WriterNoAggregationManager[K, V, C](shuffleDependency: ShuffleDependency[K, V, C],
                                          serializer: Serializer,
                                          bufferOptions: BufferManagerOptions,
                                          writeClient: ShuffleDataWriter,
                                          conf: SparkConf,
                                          taskMetrics: TaskMetrics)
  extends WriterBufferManager[K, V, C](writeClient, conf, shuffleDependency.partitioner.numPartitions,
    serializer, bufferOptions, taskMetrics) with Logging {

  private val createCombiner = shuffleDependency.aggregator.get.createCombiner

  override def addRecord(partitionId: Int, record: Product2[K, V]): Unit = {
    val combinedValue = createCombiner(record._2)
    addRecordImpl(partitionId, (record._1, combinedValue))
  }
}