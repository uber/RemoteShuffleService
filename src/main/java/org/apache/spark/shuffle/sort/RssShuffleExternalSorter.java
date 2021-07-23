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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.shuffle.RssOpts;
import org.apache.spark.shuffle.RssOpts$;
import org.apache.spark.shuffle.rss.RssShuffleWriteManager;
import org.apache.spark.shuffle.rss.ShuffleWriteMetadata;
import org.apache.spark.shuffle.rss.ShuffleWriteTimeMetadata;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConverters;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;


final class RssShuffleExternalSorter extends MemoryConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RssShuffleExternalSorter.class);

    @VisibleForTesting
    static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

    private final int numPartitions;
    private final TaskMemoryManager taskMemoryManager;
    private final BlockManager blockManager;
    private final TaskContext taskContext;
    private final ShuffleWriteMetrics writeMetrics;

    /**
     * Force this sorter to spill when there are this many elements in memory.
     */
    private final int numElementsForSpillThreshold;

    private long totalSizeCurrent = 0l;

    private long sizeThreshold;

    long numRecords = 0l;

    private final long writeBufferSize;

    private RssShuffleWriteManager writeManager;

    /**
     * Memory pages that hold the records being sorted. The pages in this list are freed when
     * spilling, although in principle we could recycle these pages across spills (on the other hand,
     * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
     * itself).
     */
    private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

    private final LinkedList<SpillInfo> spills = new LinkedList<>();

    /**
     * Peak memory used by this sorter so far, in bytes.
     **/
    private long peakMemoryUsedBytes;

    // These variables are reset after spilling:
    @Nullable
    private RssShuffleInMemorySorter inMemSorter;
    @Nullable
    private MemoryBlock currentPage = null;
    private long pageCursor = -1;

    long bytesWritten = 0l;
    long totalSerializationTime = 0l;
    int maxRecordSize = Integer.MIN_VALUE;

    RssShuffleExternalSorter(
            TaskMemoryManager memoryManager,
            BlockManager blockManager,
            TaskContext taskContext,
            int initialSize,
            int numPartitions,
            SparkConf conf,
            ShuffleWriteMetrics writeMetrics,
            RssShuffleWriteManager writer) {
        super(memoryManager,
                (int) Math.min(RssPackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
                memoryManager.getTungstenMemoryMode());
        this.taskMemoryManager = memoryManager;
        this.blockManager = blockManager;
        this.taskContext = taskContext;
        this.numPartitions = numPartitions;
        this.numElementsForSpillThreshold =
                (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
        this.writeMetrics = writeMetrics;

        this.inMemSorter = new RssShuffleInMemorySorter(
                this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true), true);
        this.peakMemoryUsedBytes = getMemoryUsage();
        this.writeManager = writer;
        this.writeBufferSize = conf.getLong("spark.shuffle.rss.unsafe.writer.bufferSize", 5 * 1024 * 1024l);
        this.sizeThreshold = Long.MAX_VALUE;
    }

    private void writeIterator() {

        // TODO: Fix metrics
        final ShuffleWriteMetrics writeMetricsToUse;

        writeMetricsToUse = writeMetrics;

        // This call performs the actual sort.
        final RssShuffleInMemorySorter.RssShuffleSorterIterator sortedRecords =
                inMemSorter.getSortedIterator();

        Iterator<Tuple3<Object, byte[], Integer>> itr = new Iterator<Tuple3<Object, byte[], Integer>>() {

            int currentPartition = -1;
            int currentPartitionDataRemaining = 0;
            long recordReadPosition = 0l;
            int arrayFillPosition = 0;
            Object recordPage = null;
            final int uaoSize = UnsafeAlignedOffset.getUaoSize();
            byte[] byteArr = new byte[(int) writeBufferSize];

            int prevPartition = -1;

            @Override
            public boolean hasNext() {
                if (currentPartition == -1 || currentPartitionDataRemaining <= 0) {
                    if (sortedRecords.hasNext()) {
                        sortedRecords.loadNext();
                        final int partition = sortedRecords.packedRecordPointer.getPartitionId();
                        assert (partition >= currentPartition);
                        if (currentPartition != -1) {
                            prevPartition = currentPartition;
                        } else {
                            prevPartition = partition;
                        }
                        currentPartition = partition;

                        final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
                        recordPage = taskMemoryManager.getPage(recordPointer);
                        final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
                        currentPartitionDataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
                        recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            }

            @Override
            public Tuple3<Object, byte[], Integer> next() {

                boolean condition = true;
                int partitionIdToSpill = currentPartition;
                arrayFillPosition = 0;

                while (condition) {

                    final int toTransfer = Math.min((int) writeBufferSize - arrayFillPosition, currentPartitionDataRemaining);

                    // ToDo: This is an issue when the records are too large
                    if (toTransfer < currentPartitionDataRemaining) {
                        condition = false;
                    } else {
                        Platform.copyMemory(recordPage, recordReadPosition, byteArr,
                                Platform.BYTE_ARRAY_OFFSET + arrayFillPosition, toTransfer);
                        recordReadPosition += toTransfer;
                        currentPartitionDataRemaining -= toTransfer;
                        bytesWritten += toTransfer;
                        arrayFillPosition += toTransfer;

                        if (arrayFillPosition >= writeBufferSize) {
                            // There is no more space left in byteArr
                            assert (arrayFillPosition == byteArr.length);
                            assert (arrayFillPosition == writeBufferSize);
                            partitionIdToSpill = currentPartition;
                            condition = false;
                        } else if (hasNext()) {
                            if (currentPartition != prevPartition) {
                                // byteArr has more space but there no data for the previous partition anymore
                                partitionIdToSpill = prevPartition;
                                condition = false;
                            } else {
                                // byteArr has more space and there is more data in the current partition,
                                // lets fetch more
                                condition = true;
                            }
                        } else {
                            // there are no records left anymore. Lets send whatever we got
                            partitionIdToSpill = currentPartition;
                            condition = false;
                        }
                    }
                }
                return new Tuple3<Object, byte[], Integer>(partitionIdToSpill, byteArr, arrayFillPosition);
            }
        };
        this.writeManager.sendDataBlocks(JavaConverters.asScalaIteratorConverter(itr).asScala());
    }

    /**
     * Sort and spill the current records in response to memory pressure.
     */
    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
            return 0L;
        }

        logger.info("Thread {} spilling sort data of {} to RSS servers ({} {} so far)",
                Thread.currentThread().getId(),
                Utils.bytesToString(getMemoryUsage()),
                spills.size(),
                spills.size() > 1 ? " times" : " time");

        writeIterator();

        final long spillSize = freeMemory();
        inMemSorter.reset();
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
        totalSizeCurrent = 0;
        return spillSize;
        // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
        // records. Otherwise, if the task is over allocated memory, then without freeing the memory
        // pages, we might not be able to get memory for the pointer array.
    }

    void reset() {
        inMemSorter.reset();
    }

    private long getMemoryUsage() {
        long totalPageSize = 0;
        for (MemoryBlock page : allocatedPages) {
            totalPageSize += page.size();
        }
        return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
    }

    private void updatePeakMemoryUsed() {
        long mem = getMemoryUsage();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    private long freeMemory() {
        updatePeakMemoryUsed();
        long memoryFreed = 0;
        for (MemoryBlock block : allocatedPages) {
            memoryFreed += block.size();
            freePage(block);
        }
        allocatedPages.clear();
        currentPage = null;
        pageCursor = 0;
        return memoryFreed;
    }

    /**
     * Force all memory to be deleted; called by shuffle error-handling code.
     */
    public void cleanupResources() {
        freeMemory();
        if (inMemSorter != null) {
            inMemSorter.free();
            inMemSorter = null;
        }
    }

    /**
     * Checks whether there is enough space to insert an additional record in to the sort pointer
     * array and grows the array if additional space is required. If the required space cannot be
     * obtained, then the in-memory data will be spilled to disk.
     */
    private void growPointerArrayIfNecessary() throws IOException {
        assert (inMemSorter != null);
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
            long used = inMemSorter.getMemoryUsage();
            LongArray array;
            try {
                // could trigger spilling
                array = allocateArray(used / 8 * 2);
            } catch (TooLargePageException e) {
                // The pointer array is too big to fix in a single page, spill.
                spill();
                return;
            } catch (SparkOutOfMemoryError e) {
                // should have trigger spilling
                if (!inMemSorter.hasSpaceForAnotherRecord()) {
                    logger.error("Unable to grow the pointer array");
                    throw e;
                }
                return;
            }
            // check if spilling is triggered or not
            if (inMemSorter.hasSpaceForAnotherRecord()) {
                freeArray(array);
            } else {
                inMemSorter.expandPointerArray(array);
            }
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
    private void acquireNewPageIfNecessary(int required) {
        if (currentPage == null ||
                pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
            // TODO: try to find space in previous pages
            currentPage = allocatePage(required);
            pageCursor = currentPage.getBaseOffset();
            allocatedPages.add(currentPage);
        }
    }

    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
            throws IOException {

        // for tests
        assert (inMemSorter != null);
        if (inMemSorter.numRecords() >= numElementsForSpillThreshold || totalSizeCurrent >= sizeThreshold) {
            logger.info("Spilling data because number of spilledRecords crossed the threshold or Most likely the size is > 5MB " +
                    numElementsForSpillThreshold + " " + totalSizeCurrent);
            spill();
            totalSizeCurrent = 0;
        }

        Long startTime = System.nanoTime();
        growPointerArrayIfNecessary();
        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        // Need 4 or 8 bytes to store the record length.
        final int required = length + uaoSize;
        // TODO: This throws OOM when page could not be found. Handle this
        acquireNewPageIfNecessary(required);

        assert (currentPage != null);
        final Object base = currentPage.getBaseObject();
        final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
        UnsafeAlignedOffset.putSize(base, pageCursor, length);
        pageCursor += uaoSize;
        Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
        pageCursor += length;
        totalSizeCurrent += length;
        numRecords += 1;
//        maxRecordSize = Math.max(maxRecordSize, length);
        inMemSorter.insertRecord(recordAddress, partitionId);
        totalSerializationTime += System.nanoTime() - startTime;
    }


    /**
     * Close the sorter, causing any buffered data to be sorted and written out to disk.
     *
     * @throws IOException
     */
    public Tuple2<ShuffleWriteMetadata, ShuffleWriteTimeMetadata> spillAndFreeMemory() throws IOException {
        ShuffleWriteMetadata writeMetadata = null;
        ShuffleWriteTimeMetadata writeTimeMetadata = null;
        if (inMemSorter != null) {
            writeIterator();
            writeMetadata = new ShuffleWriteMetadata(0l, 0l, bytesWritten, writeManager.getNumOfSpills(), writeManager.partitionLengths());
            writeTimeMetadata = new ShuffleWriteTimeMetadata(totalSerializationTime, writeManager.totalCompressionTime(), writeManager.totalUploadTime(), writeManager.totalMemoryFetchTime(), writeManager.totalSendDataBlockTime());
            freeMemory();
            inMemSorter.free();
            inMemSorter = null;
        }
        return new Tuple2(writeMetadata, writeTimeMetadata);
    }
}
