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

package org.apache.spark.shuffle.sort

import javax.annotation.Nullable
import java.io.IOException
import java.util.{Collections, Iterator, LinkedList}
import java.util
import org.apache.spark.shuffle.RssOpts
import org.apache.spark.shuffle.RssOpts$
import org.apache.spark.shuffle.rss.{RssShuffleWriteManager, ShuffleWriteMetadata, ShuffleWriteTimeMetadata}

import scala.Tuple2
import scala.Tuple3
import scala.collection.JavaConverters
import com.google.common.annotations.VisibleForTesting
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.config.package$
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.memory.TooLargePageException
import org.apache.spark.storage.BlockManager
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.UnsafeAlignedOffset
import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Utils


/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link RssUnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
object RssShuffleExternalSorter {
  private val logger = LoggerFactory.getLogger(classOf[RssShuffleExternalSorter])
  @VisibleForTesting private[sort] val DISK_WRITE_BUFFER_SIZE = 1024 * 1024
}

final class RssShuffleExternalSorter private[sort](val taskMemoryManager: TaskMemoryManager, val blockManager: BlockManager, val taskContext: TaskContext, val initialSize: Int, val numPartitions: Int, val conf: SparkConf, val writeMetrics: ShuffleWriteMetrics, var writeManager: RssShuffleWriteManager[_, _, _]) extends MemoryConsumer(memoryManager, Math.min(RssPackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes).toInt, memoryManager.getTungstenMemoryMode) {
  this.numElementsForSpillThreshold = conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD).asInstanceOf[Int]
  this.inMemSorter = new RssShuffleInMemorySorter(this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true), true)
  this.peakMemoryUsedBytes = getMemoryUsage
  this.diskWriteBufferSize = conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE).asInstanceOf[Long].toInt
  this.writeBufferSize = conf.getLong("spark.shuffle.rss.unsafe.writer.bufferSize", 5 * 1024 * 1024l)
  this.sizeThreshold = Long.MAX_VALUE
  /**
   * Force this sorter to spill when there are this many elements in memory.
   */
  final private var numElementsForSpillThreshold = 0
  private var totalSizeCurrent = 0l
  private var sizeThreshold = 0L
  private[sort] var numRecords = 0l
  /**
   * The buffer size to use when writing the sorted records to an on-disk file
   */
  final private var diskWriteBufferSize = 0
  final private var writeBufferSize = 0L
  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  final private val allocatedPages = new util.LinkedList[MemoryBlock]
  final private val spills = new util.LinkedList[SpillInfo]
  /**
   * Peak memory used by this sorter so far, in bytes.
   * */
  private var peakMemoryUsedBytes = 0L
  // These variables are reset after spilling:
  @Nullable private var inMemSorter = null
  @Nullable private var currentPage = null
  private var pageCursor = -1
  private[sort] var bytesWritten = 0l
  private[sort] var totalSerializationTime = 0l
  private[sort] val maxRecordSize = Integer.MIN_VALUE

  /**
   * Sorts the in-memory records and writes the sorted records to an on-disk file.
   * This method does not free the sort data structures.
   */
  private def writeIterator(): Unit = { // TODO: Fix metrics
    val writeMetricsToUse = null
    writeMetricsToUse = writeMetrics
    // This call performs the actual sort.
    val sortedRecords = inMemSorter.getSortedIterator
    val itr = new util.Iterator[Tuple3[AnyRef, Array[Byte], Integer]]() {
      private[sort] var currentPartition = -1
      private[sort] var currentPartitionDataRemaining = 0
      private[sort] var recordReadPosition = 0l
      private[sort] var arrayFillPosition = 0
      private[sort] var recordPage = null
      final private[sort] val uaoSize = UnsafeAlignedOffset.getUaoSize
      private[sort] val byteArr = new Array[Byte](writeBufferSize.toInt)
      private[sort] var prevPartition = -1

      override def hasNext: Boolean = if (currentPartition == -1 || currentPartitionDataRemaining <= 0) if (sortedRecords.hasNext) {
        sortedRecords.loadNext()
        val partition = sortedRecords.packedRecordPointer.getPartitionId
        //                        System.out.println("######### " + partition);
        assert(partition >= currentPartition)
        if (currentPartition != -1) prevPartition = currentPartition
        else prevPartition = partition
        currentPartition = partition
        val recordPointer = sortedRecords.packedRecordPointer.getRecordPointer
        recordPage = taskMemoryManager.getPage(recordPointer)
        val recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer)
        currentPartitionDataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage)
        recordReadPosition = recordOffsetInPage + uaoSize // skip over record length

        true
      }
      else false
      else true

      override def next: Tuple3[AnyRef, Array[Byte], Integer] = {
        var condition = true
        var partitionIdToSpill = currentPartition
        //                byteArr = new byte[(int) writeBufferSize*2];
        //                byte[] byteArrTemp = new byte[(int) writeBufferSize];
        arrayFillPosition = 0
        while ( {
          condition
        }) {
          val toTransfer = Math.min(writeBufferSize.toInt - arrayFillPosition, currentPartitionDataRemaining)
          // ToDo: This is an issue when the records are too large
          if (toTransfer < currentPartitionDataRemaining) condition = false
          else {
            Platform.copyMemory(recordPage, recordReadPosition, byteArr, Platform.BYTE_ARRAY_OFFSET + arrayFillPosition, toTransfer)
            //                    Platform.copyMemory(recordPage, recordReadPosition, byteArrTemp,
            //                            Platform.BYTE_ARRAY_OFFSET, toTransfer);
            //                    for (int i = 0; i < toTransfer; i++) {
            //                        byteArrTemp[arrayFillPosition+i] = Platform.getByte(recordPage, recordReadPosition+i);
            //                    }
            recordReadPosition += toTransfer
            currentPartitionDataRemaining -= toTransfer
            bytesWritten += toTransfer
            arrayFillPosition += toTransfer
            if (arrayFillPosition >= writeBufferSize) { // I don't have space anymore
              assert(arrayFillPosition == byteArr.length)
              assert(arrayFillPosition == writeBufferSize)
              partitionIdToSpill = currentPartition
              condition = false
            }
            else if (hasNext) if (currentPartition != prevPartition) { // I have more space but there no data for the previous partition anymore
              partitionIdToSpill = prevPartition
              condition = false
            }
            else { // I have more space and there is more data in the partition, lets fetch more
              condition = true
            }
            else { // there are no records left anymore. Lets send whatever we got
              partitionIdToSpill = currentPartition
              condition = false
            }
          }
        }
        //                arrayFillPosition = 0;
        new Tuple3[AnyRef, Array[Byte], Integer](partitionIdToSpill, byteArr, arrayFillPosition)
      }
    }
    this.writeManager.sendDataBlocks(JavaConverters.asScalaIteratorConverter(itr).asScala)
  }

  private def writeIteratorC(): Unit = {
    val writeMetricsToUse = null
    writeMetricsToUse = writeMetrics
    val sortedRecords = inMemSorter.getSortedIterator
    val itr = new util.Iterator[Tuple3[AnyRef, Array[Byte], Integer]]() {
      private[sort] var currentPartition = -1
      private[sort] var currentPartitionDataRemaining = 0
      private[sort] var recordReadPosition = 0l
      private[sort] val arrayFillPosition = 0
      private[sort] var recordPage = null
      final private[sort] val uaoSize = UnsafeAlignedOffset.getUaoSize
      private[sort] val byteArr = new Array[Byte](writeBufferSize.toInt)
      private[sort] var prevPartition = -1

      override def hasNext: Boolean = if (currentPartition == -1 || currentPartitionDataRemaining <= 0) if (sortedRecords.hasNext) {
        sortedRecords.loadNext()
        val partition = sortedRecords.packedRecordPointer.getPartitionId
        assert(partition >= currentPartition)
        if (currentPartition != -1) prevPartition = currentPartition
        else prevPartition = partition
        currentPartition = partition
        val recordPointer = sortedRecords.packedRecordPointer.getRecordPointer
        recordPage = taskMemoryManager.getPage(recordPointer)
        val recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer)
        currentPartitionDataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage)
        recordReadPosition = recordOffsetInPage + uaoSize
        true
      }
      else false
      else true

      override def next: Tuple3[AnyRef, Array[Byte], Integer] = { //                byteArr.c;
        //                while (hasNext() && )
        //                final int toTransfer = Math.min(diskWriteBufferSize, currentPartitionDataRemaining);
        val toTransfer = currentPartitionDataRemaining
        // TODO: These are O(N) assignments
        val byteArr = new Array[Byte](toTransfer)
        //                Object 0 = Platform.getObjectVolatile(recordPage,  )
        //                Array<Byte> byteArr = new Array<Byte>(toTransfer);
        Platform.copyMemory(recordPage, recordReadPosition, byteArr, Platform.BYTE_ARRAY_OFFSET, toTransfer)
        //                for (int i = 0; i < toTransfer; i++) {
        //                    byteArr.update(i, Platform.getByte(recordPage, recordReadPosition));
        //                    byteArr[i] = Platform.getByte(recordPage, recordReadPosition);
        //                    recordReadPosition += 1l;
        //                }
        //                    System.out.println(byteArr[i]);
        recordReadPosition += toTransfer
        currentPartitionDataRemaining -= toTransfer
        bytesWritten += toTransfer
        new Tuple3[AnyRef, Array[Byte], Integer](currentPartition, byteArr, -1)
      }
    }
    this.writeManager.sendDataBlocks(JavaConverters.asScalaIteratorConverter(itr).asScala)
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @throws[IOException]
  override def spill(size: Long, trigger: MemoryConsumer): Long = { //        logger.info(">>>>>>> Spilling");
    if ((trigger ne this) || inMemSorter == null || inMemSorter.numRecords == 0) return 0L
    RssShuffleExternalSorter.logger.info("Thread {} spilling sort data of {} to RSS servers ({} {} so far)", Thread.currentThread.getId, Utils.bytesToString(getMemoryUsage), spills.size, if (spills.size > 1) " times"
    else " time")
    writeIterator()
    val spillSize = freeMemory
    inMemSorter.reset()
    taskContext.taskMetrics.incMemoryBytesSpilled(spillSize)
    totalSizeCurrent = 0
    spillSize
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
  }

  private[sort] def reset(): Unit = {
    inMemSorter.reset()
  }

  private def getMemoryUsage = {
    var totalPageSize = 0
    import scala.collection.JavaConversions._
    for (page <- allocatedPages) {
      totalPageSize += page.size
    }
    (if (inMemSorter == null) 0
    else inMemSorter.getMemoryUsage) + totalPageSize
  }

  private def updatePeakMemoryUsed(): Unit = {
    val mem = getMemoryUsage
    if (mem > peakMemoryUsedBytes) peakMemoryUsedBytes = mem
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  private[sort] def getPeakMemoryUsedBytes = {
    updatePeakMemoryUsed()
    peakMemoryUsedBytes
  }

  private def freeMemory = {
    updatePeakMemoryUsed()
    var memoryFreed = 0
    import scala.collection.JavaConversions._
    for (block <- allocatedPages) {
      memoryFreed += block.size
      freePage(block)
    }
    allocatedPages.clear()
    currentPage = null
    pageCursor = 0
    memoryFreed
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  def cleanupResources(): Unit = {
    freeMemory
    if (inMemSorter != null) {
      inMemSorter.free()
      inMemSorter = null
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  @throws[IOException]
  private def growPointerArrayIfNecessary(): Unit = {
    assert(inMemSorter != null)
    if (!inMemSorter.hasSpaceForAnotherRecord) {
      val used = inMemSorter.getMemoryUsage
      var array = null
      try // could trigger spilling
        array = allocateArray(used / 8 * 2)
      catch {
        case e: TooLargePageException =>
          // The pointer array is too big to fix in a single page, spill.
          spill()
          return
        case e: SparkOutOfMemoryError =>
          // should have trigger spilling
          if (!inMemSorter.hasSpaceForAnotherRecord) {
            RssShuffleExternalSorter.logger.error("Unable to grow the pointer array")
            throw e
          }
          return
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord) freeArray(array)
      else inMemSorter.expandPointerArray(array)
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                 the record size. This must be less than or equal to the page size (records
   *                 that exceed the page size are handled via a different code path which uses
   *                 special overflow pages).
   */
  private def acquireNewPageIfNecessary(required: Int): Unit = {
    if (currentPage == null || pageCursor + required > currentPage.getBaseOffset + currentPage.size) { // TODO: try to find space in previous pages
      currentPage = allocatePage(required)
      pageCursor = currentPage.getBaseOffset
      allocatedPages.add(currentPage)
    }
  }

  @throws[IOException]
  def insertRecord(recordBase: Any, recordOffset: Long, length: Int, partitionId: Int): Unit = { // for tests
    assert(inMemSorter != null)
    if (inMemSorter.numRecords >= numElementsForSpillThreshold || totalSizeCurrent >= sizeThreshold) {
      RssShuffleExternalSorter.logger.info("Spilling data because number of spilledRecords crossed the threshold or Most likely the size is > 5MB " + numElementsForSpillThreshold + " " + totalSizeCurrent)
      spill()
      totalSizeCurrent = 0
    }
    val startTime = System.nanoTime
    growPointerArrayIfNecessary()
    val uaoSize = UnsafeAlignedOffset.getUaoSize
    // Need 4 or 8 bytes to store the record length.
    val required = length + uaoSize
    // TODO: This throws OOM when page could not be found. Handle this
    acquireNewPageIfNecessary(required)
    assert(currentPage != null)
    val base = currentPage.getBaseObject
    val recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor)
    UnsafeAlignedOffset.putSize(base, pageCursor, length)
    pageCursor += uaoSize
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length)
    pageCursor += length
    totalSizeCurrent += length
    numRecords += 1
    //        maxRecordSize = Math.max(maxRecordSize, length);
    inMemSorter.insertRecord(recordAddress, partitionId)
    totalSerializationTime += System.nanoTime - startTime
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  @throws[IOException]
  def spillAndFreeMemory: Tuple2[ShuffleWriteMetadata, ShuffleWriteTimeMetadata] = {
    var writeMetadata = null
    var writeTimeMetadata = null
    if (inMemSorter != null) { // Do not count the final file towards the spill count.
      writeIterator()
      writeMetadata = new ShuffleWriteMetadata(0l, 0l, bytesWritten, writeManager.getNumOfSpills, writeManager.partitionLengths)
      writeTimeMetadata = new ShuffleWriteTimeMetadata(totalSerializationTime, writeManager.totalCompressionTime, writeManager.totalUploadTime, writeManager.totalMemoryFetchTime, writeManager.totalSendDataBlockTime)
      freeMemory
      inMemSorter.free()
      inMemSorter = null
    }
    new Tuple2[_, _](writeMetadata, writeTimeMetadata)
  }
}
