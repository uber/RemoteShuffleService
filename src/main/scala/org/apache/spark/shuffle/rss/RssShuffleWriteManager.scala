package org.apache.spark.shuffle.rss

import com.uber.rss.clients.ShuffleDataWriter
import net.jpountz.lz4.LZ4Factory
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.RssOpts

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.collection.mutable

case class ShuffleWriteTimeMetadata(serializationTime: Long, compressionTime: Long, uploadTime: Long,
                                    memoryFetchTime: Long, sendDataBlockTime: Long = 0l)

case class ShuffleWriteMetadata(recordsRead: Long, recordsWritten: Long, bytesWritten: Long,
                                numberOfSpills: Long, partitionLengths: Array[Long])

abstract class RssShuffleWriteManager[K, V, C](writeClient: ShuffleDataWriter, conf: SparkConf, numPartitions: Int)
  extends Logging {

  private val compressor = LZ4Factory.fastestInstance.fastCompressor

  private var numberOfSpills: Long = 0l

  private val writeClientCloseLock = new Object()

  var totalCompressionTime: Long = 0
  var totalUploadTime: Long = 0
  var totalMemoryFetchTime: Long = 0
  var totalBufferedSize: Long = 0
  var totalSendDataBlockTime: Long = 0

  private val writerBufferSize = conf.get(RssOpts.unsafeShuffleWriterBufferSize);

  def getNumOfSpills(): Long = {
    numberOfSpills
  }

  val partitionLengths: Array[Long] = Array.fill[Long](numPartitions)(0L)

  var sendDataBlockCnt: Long = 0

  def sendDataBlocks(dataBlocksItr: Iterator[(Int, Array[Byte], Int)]) = {
    val sendDataBlockStartTime: Long = System.nanoTime()
    var fetchStartTime: Long = System.nanoTime()

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
        sendDataBlockCnt += 1
        val dataBlock = createDataBlock(data, sizeToSend)
        val startTime = System.nanoTime()
        writeClient.writeDataBlock(partitionId, dataBlock)
        totalUploadTime += System.nanoTime() - startTime
        numberOfSpills += 1
      }
      fetchStartTime = System.nanoTime()
    }
    totalSendDataBlockTime += System.nanoTime() - sendDataBlockStartTime
  }

  private def createDataBlock(buffer: Array[Byte], srcLen: Int = 0): ByteBuffer = {
    val startTime = System.nanoTime()
    val uncompressedByteCount = srcLen
    val compressedBuffer = new Array[Byte](compressor.maxCompressedLength(uncompressedByteCount))
    val compressedByteCount = compressor.compress(buffer, 0, srcLen, compressedBuffer, 0)
    val dataBlockByteBuffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + compressedByteCount)
    dataBlockByteBuffer.putInt(compressedByteCount)
    dataBlockByteBuffer.putInt(uncompressedByteCount)
    dataBlockByteBuffer.put(compressedBuffer, 0, compressedByteCount)
    dataBlockByteBuffer.flip
    totalCompressionTime += System.nanoTime() - startTime
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

  def stop(): Unit = {}

  def getShuffleWriteTimeMetadata(): ShuffleWriteTimeMetadata

  def getShuffleWriteMetadata(): ShuffleWriteMetadata

  def addRecord(partitionId: Int, record: Product2[K, V]): Unit

  def collectionSizeInBytes: Int

  def clear(): Unit

  def recordsWritten: Int

  def reductionFactor: Double
}