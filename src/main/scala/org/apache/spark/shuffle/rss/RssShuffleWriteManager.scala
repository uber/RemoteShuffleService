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

import org.apache.spark.internal.Logging

/**
 * @param serializationTime Total time for serializing records
 * @param compressionTime Total time for compressing records
 * @param uploadTime Total time for sending shuffle data to servers
 * @param memoryFetchTime Total time for copying data to intermittent buffers before sending to servers
 * @param sendDataBlockTime Total time for spill
 */
case class ShuffleWriteTimeMetadata(serializationTime: Long, compressionTime: Long, uploadTime: Long,
                                    memoryFetchTime: Long, sendDataBlockTime: Long = 0l)

/**
 * @param recordsRead Total records read
 * @param recordsWritten Total records written to servers
 * @param numberOfSpills Total number of spills
 * @param partitionLengths Total bytes written for each partition
 * @param reductionFactor Reduction percentage in mappers (0 when no map side aggregation is involved)
 * @param numberOfNetworkWrites Total number of network writes
 */
case class ShuffleWriteMetadata(recordsRead: Long, recordsWritten: Long, numberOfSpills: Long,
                                partitionLengths: Array[Long], reductionFactor: Double,
                                numberOfNetworkWrites: Long)

/**
 * The base interface for pre-processing and holding shuffle data in memory before sending it to RSS servers
 */
abstract class RssShuffleWriteManager[K, V, C](shuffleWriteCurator: ShuffleWriteCurator) extends Logging {

  /**
   * Process new record and spill the data read so far if needed be. The caller will call finish() endpoint when
   * all the records have been read. The implementation is supposed to orchestrate any other size, etc based
   * intermittent spilling within this call
   */
  def addRecord(partitionId: Int, record: Product2[K, V]): Unit

  /**
   * Called when all the records have been read. Implementation should spill all contents of the collection on finish()
   * method call
   */
  def finish(): Unit

  /**
   * Current size of the collection in memory
   */
  def collectionSizeInBytes: Int

  def getShuffleWriteTimeMetadata(): ShuffleWriteTimeMetadata

  def getShuffleWriteMetadata(): ShuffleWriteMetadata

  /**
   * Hook on task stop (success or failure)
   */
  def stop(success: Boolean): Unit = {
    shuffleWriteCurator.closeWriteClientAsync()
  }
}
