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
 * [[org.apache.spark.shuffle.rss.WriterAggregationMapper]] when the reduction
 * factor is very low.
 */
class WriterAggregationManager[K, V, C](shuffleWriteCurator: ShuffleWriteCurator,
                                        shuffleDependency: ShuffleDependency[K, V, C],
                                        serializer: Serializer,
                                        bufferOptions: BufferManagerOptions,
                                        conf: SparkConf)
  extends RssShuffleWriteManager[K, V, C](shuffleWriteCurator) {

  private var recordsRead: Int = 0

  private val aggImpl = new WriterAggregationImpl(shuffleWriteCurator, shuffleDependency,
    serializer, bufferOptions, conf)

  def recordsWritten: Int = aggImpl.recordsWritten

  // Fraction of the total records which were aggregated
  def reductionFactor: Double = {
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

  override def addRecord(partitionId: Int, record: Product2[K, V]): Unit = {
    recordsRead += 1
    aggImpl.insert(partitionId, record)
  }

  override def finish(): Unit = {
    if (collectionSizeInBytes != 0) {
      aggImpl.spillMap()
    }
  }

  override def collectionSizeInBytes: Int = {
    aggImpl.collectionSizeInBytes
  }

  override def getShuffleWriteTimeMetadata(): ShuffleWriteTimeMetadata = {
    aggImpl.shuffleWriteTimeMetadata
  }

  override def getShuffleWriteMetadata(): ShuffleWriteMetadata = {
    aggImpl.shuffleWriteMetrics.copy(reductionFactor = reductionFactor)
  }
}
