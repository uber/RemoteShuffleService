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

import net.jpountz.lz4.LZ4Factory

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

/**
 * ShuffleWriteCurator acts as a bridge between the shuffle write managers which hold the data in memory and
 * shuffle write clients which send the data to RSS servers
 */
class ShuffleWriteCurator(writeClient: ShuffleDataWriter, numPartitions: Int) {

  private val compressor = LZ4Factory.fastestInstance.fastCompressor
  private val writeClientCloseLock = new Object()

  // One spill can include data from multiple partitions
  var numberOfSpills: Long = 0l
  // One network write will have data from single partition, so numberOfNetworkWrites per spill >= 1
  var numberOfNetworkWrites: Long = 0l
  var totalCompressionTime: Long = 0l
  var totalUploadTime: Long = 0l
  var totalMemoryFetchTime: Long = 0l
  var totalBufferedSize: Long = 0l
  var totalSendDataBlockTime: Long = 0l

  val partitionLengths: Array[Long] = Array.fill[Long](numPartitions)(0L)

  def sendDataBlocks(dataBlocksItr: Iterator[(Int, Array[Byte], Int)]): Unit = {
    val sendDataBlockStartTime: Long = System.nanoTime()
    var fetchStartTime: Long = System.nanoTime()
    var shuffleDataWrittenForAnyPartition = false

    while (dataBlocksItr.hasNext) {
      val block = dataBlocksItr.next()
      val partitionId = block._1;
      val data = block._2;
      if (data != null && data.length > 0) {
        val sizeToSend = if (block._3 == -1) {
          data.length
        } else {
          block._3
        }
        totalMemoryFetchTime += System.nanoTime() - fetchStartTime
        val dataBlock = createDataBlock(data, sizeToSend)
        val uploadStartTime = System.nanoTime()
        writeClient.writeDataBlock(partitionId, dataBlock)
        partitionLengths(partitionId) += sizeToSend
        totalUploadTime += System.nanoTime() - uploadStartTime
        numberOfNetworkWrites += 1
        shuffleDataWrittenForAnyPartition = true
      }
      fetchStartTime = System.nanoTime()
    }
    if (shuffleDataWrittenForAnyPartition) {
      numberOfSpills += 1
    }
    totalSendDataBlockTime += System.nanoTime() - sendDataBlockStartTime
  }

  private def createDataBlock(buffer: Array[Byte], srcLen: Int = 0): ByteBuffer = {
    val compressionStartTime = System.nanoTime()
    val uncompressedByteCount = srcLen
    val compressedBuffer = new Array[Byte](compressor.maxCompressedLength(uncompressedByteCount))
    val compressedByteCount = compressor.compress(buffer, 0, srcLen, compressedBuffer, 0)
    val dataBlockByteBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + compressedByteCount)
    dataBlockByteBuffer.putInt(compressedByteCount)
    dataBlockByteBuffer.putInt(uncompressedByteCount)
    dataBlockByteBuffer.put(compressedBuffer, 0, compressedByteCount)
    dataBlockByteBuffer.flip
    totalCompressionTime += System.nanoTime() - compressionStartTime
    dataBlockByteBuffer
  }

  def closeWriteClientAsync() = {
    CompletableFuture.runAsync(new Runnable {
      override def run(): Unit = {
        writeClientCloseLock.synchronized {
          writeClient.close()
        }
      }
    })
  }
}
