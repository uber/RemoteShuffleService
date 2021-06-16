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
import org.apache.spark.serializer.Serializer

/**
 * An aggregation manager which tries to do a best effort map side aggregation.
 * See [[org.apache.spark.shuffle.rss.WriterAggregationImpl]]
 *
 * In future, this abstraction will be used to fallback to
 * [[org.apache.spark.shuffle.rss.WriterNoAggregationManager]] when the reduction
 * factor is very low.
 */
class WriterAggregationManager[K, V, C](shuffleDependency: ShuffleDependency[K, V, C],
                                        serializer: Serializer,
                                        bufferOptions: BufferManagerOptions,
                                        conf: SparkConf)
  extends WriteBufferManager[K, V, C](serializer, bufferOptions) with Logging {

  private var recordsRead: Int = 0

  private val aggImpl = new WriterAggregationImpl(shuffleDependency, serializer, bufferOptions, conf)

  override def recordsWritten: Int = aggImpl.recordsWritten

  // Fraction of the total records which were aggregated
  override def reductionFactor: Double = {
    if (recordsRead == 0) {
      // Case of an empty partition
      0.0
    } else {
      // Since the records in the map are already aggregated, total records post
      // aggregation would be sum of records written to RSS servers so far (because of spill if any)
      // and number of records in the map
      val recordsCountPostAgg = recordsWritten + aggImpl.numberOfRecordsInMap
      assert(recordsRead >= recordsCountPostAgg)
      1.0 - (recordsCountPostAgg / recordsRead.toDouble)
    }
  }


  override def addRecord(partitionId: Int, record: Product2[K, V]): Seq[(Int, Array[Byte])] = {
    recordsRead += 1
    aggImpl.addRecord(partitionId, record)
  }

  override def clear(): Seq[(Int, Array[Byte])] = {
    aggImpl.spillMap()
  }

  override def collectionSizeInBytes: Int = {
    aggImpl.collectionSizeInBytes
  }
}