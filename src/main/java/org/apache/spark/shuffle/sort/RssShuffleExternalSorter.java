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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import com.uber.rss.exceptions.RssMapperDataMismatchException;
import com.uber.rss.exceptions.RssUnsafeShuffleWriteException;
import scala.collection.JavaConverters;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.shuffle.rss.ShuffleWriteCurator;
import org.apache.spark.shuffle.rss.ShuffleWriteMetadata;
import org.apache.spark.shuffle.rss.ShuffleWriteTimeMetadata;
import scala.Tuple2;
import scala.Tuple3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;


/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link RssShuffleInMemorySorter}). The sorted records are then
 * sent to the remote RSS servers.
 * <p>
 */
final class RssShuffleExternalSorter extends MemoryConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RssShuffleExternalSorter.class);

    private final TaskMemoryManager taskMemoryManager;
    private final TaskMetrics taskMetrics;

    /**
     * Force this sorter to spill when there are this many elements in memory.
     */
    private final int numElementsForSpillThreshold;
    private final boolean useDynamicBuffer;
    private final boolean enableRecordCountMatchCheck;
    private int writeBufferSizeStatic;
    private int maxRecordSize;

    private ShuffleWriteCurator curator;

    /**
     * Memory pages that hold the records being sorted. The pages in this list are freed when
     * spilling, although in principle we could recycle these pages across spills (on the other hand,
     * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
     * itself).
     */
    private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

    private final LinkedList<SpillInfo> spills = new LinkedList<>();

    /** Peak memory used by this sorter so far, in bytes. **/
    private long peakMemoryUsedBytes;

    // These variables are reset after spilling:
    @Nullable
    private RssShuffleInMemorySorter inMemSorter;
    @Nullable
    private MemoryBlock currentPage = null;
    private long pageCursor = -1;
    private int recordsRead = 0;
    private int recordsWritten = 0;
    private long totalSerializationTime = 0l;


    RssShuffleExternalSorter(
            TaskMemoryManager memoryManager,
            TaskMetrics taskMetrics,
            int initialSize,
            SparkConf conf,
            ShuffleWriteCurator curator) {
        super(memoryManager,
                (int) Math.min(RssPackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
                memoryManager.getTungstenMemoryMode());
        this.taskMemoryManager = memoryManager;
        this.taskMetrics = taskMetrics;
        this.numElementsForSpillThreshold =
                (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());

        this.inMemSorter = new RssShuffleInMemorySorter(
                this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
        this.peakMemoryUsedBytes = getMemoryUsage();
        this.curator = curator;
        this.writeBufferSizeStatic = conf.getInt("spark.shuffle.rss.unsafe.writer.bufferSize",
                5 * 1024 * 1024);
        this.maxRecordSize = writeBufferSizeStatic;
        this.useDynamicBuffer = conf.getBoolean("spark.shuffle.rss.unsafe.useDynamicBuffer", false);
        this.enableRecordCountMatchCheck =
                conf.getBoolean("spark.shuffle.rss.unsafe.enableRecordCountCheck", true);
    }

    /**
     * Sorts the in-memory records and sends them to remote RSS servers.
     * This method does not free the sort data structures.
     */
    private void writeIterator() {

        // This call performs the actual sort.
        final RssShuffleInMemorySorter.RssShuffleSorterIterator sortedRecords =
                inMemSorter.getSortedIterator();

        Iterator<Tuple3<Object, byte[], Object>> itr = new Iterator<Tuple3<Object, byte[], Object>>() {

            private int writeBufferSize = useDynamicBuffer ? maxRecordSize : writeBufferSizeStatic;
            private int bufferVacantCapacity = writeBufferSize;
            private int currentRecPartition = -1;
            private int currentRecordSize = 0;
            private int partitionOfRecordsInBuffer = -1;
            private long currentRecordReadPosition = -1l;
            private boolean hasRecordReference = false;
            private byte[] buffer = new byte[writeBufferSize];
            private Object recordPage = null;

            private final int uaoSize = UnsafeAlignedOffset.getUaoSize();

            private int bufferFillPosition() {
                return writeBufferSize - bufferVacantCapacity;
            }

            private void referenceNextRecord() {
                sortedRecords.loadNext();
                final int partition = sortedRecords.packedRecordPointer.getPartitionId();
                if (currentRecPartition != -1) {
                    partitionOfRecordsInBuffer = currentRecPartition;
                } else {
                    partitionOfRecordsInBuffer = partition;
                }
                currentRecPartition = partition;
                final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
                recordPage = taskMemoryManager.getPage(recordPointer);
                final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
                currentRecordSize = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
                currentRecordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
                hasRecordReference = true;
            }

            private void resetRecordReference() {
                logger.debug("Resetting record reference");
                hasRecordReference = false;
            }

            private boolean canPackCurrentRecord() {
                return currentRecordSize <= bufferVacantCapacity;
            }

            private boolean isBufferEmpty() {
                return bufferVacantCapacity == writeBufferSize;
            }

            /**
             * Returns whether buffer is empty and it can not pack a currently referenced record
             */
            private boolean emptyBufferTooSmallForSingleRecord() {
                return isBufferEmpty() && (currentRecordSize > writeBufferSize);
            }

            private void copyRecordToBuffer() {
                logger.debug("Copying the record to buffer");
                Platform.copyMemory(recordPage, currentRecordReadPosition, buffer,
                        Platform.BYTE_ARRAY_OFFSET + bufferFillPosition(), currentRecordSize);
                recordsWritten += 1;
                bufferVacantCapacity -= currentRecordSize;
            }

            @Override
            public boolean hasNext() {
                return hasRecordReference || sortedRecords.hasNext();
            }


            @Override
            public Tuple3<Object, byte[], Object> next() {
                boolean shouldPackMoreRecords = true;
                partitionOfRecordsInBuffer = currentRecPartition;
                bufferVacantCapacity = writeBufferSize;

                while (shouldPackMoreRecords) {

                    if (!hasNext()) {
                        // No shuffle data remaining. Spill current contents of the buffer
                        logger.debug("No shuffle data remaining. Spill current contents of the buffer");
                        shouldPackMoreRecords = false;
                    } else {
                        if (!hasRecordReference) {
                            // Reference the next record from memory
                            logger.debug("Reference the next record from memory");
                            referenceNextRecord();
                        }

                        if (!isBufferEmpty() && (partitionOfRecordsInBuffer != currentRecPartition)) {
                            // Partition id has changed between the current and previous record
                            // Spill the current contents of the buffer first
                            logger.debug("Partition id has changed between the current and previous record. " +
                                    "Spill the current contents of the buffer");
                            shouldPackMoreRecords = false;
                        } else if (emptyBufferTooSmallForSingleRecord()) {
                            // Buffer size is too small to accommodate this record
                            throw new RssUnsafeShuffleWriteException("Serialized record size " + currentRecordSize +
                                    " is greater than the unsafe shuffle writer buffer size " + writeBufferSize +
                                    " Consider increasing the buffer size by setting conf" +
                                    " spark.shuffle.rss.unsafe.writer.bufferSize or disabling unsafe shuffle writer " +
                                    " by disabling the conf spark.shuffle.rss.useUnsafeWriter");
                        } else if (canPackCurrentRecord()) {
                            // Partition id has not changed between the current and previous record. Current record
                            // can be packed in buffer, so copy current record to buffer and fetch more records
                            copyRecordToBuffer();
                            resetRecordReference();
                        } else {
                            // Partition id has not changed between the current and previous record. But current record
                            // can be not be packed in buffer. Spill the current contents of the buffer first
                            logger.debug("Partition id has not changed between the current and previous record. " +
                                    "But current record can not be packed in buffer");
                            shouldPackMoreRecords = false;
                            // TODO: In this case, bufferVacantCapacity bytes in buffer are empty. The size of
                            //  all records should generally be the same. So, resize the byteArr in such cases to,
                            //  ceil (serialized record size * number of records read from memory so far)
                        }
                    }
                }

                return new Tuple3(partitionOfRecordsInBuffer, buffer, bufferFillPosition());
            }
        };
        this.curator.sendDataBlocks(JavaConverters.asScalaIteratorConverter(itr).asScala());
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
        // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
        // records. Otherwise, if the task is over allocated memory, then without freeing the memory
        // pages, we might not be able to get memory for the pointer array.
        taskMetrics.incMemoryBytesSpilled(spillSize);
        return spillSize;
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
     * Force all memory and spill files to be deleted; called by shuffle error-handling code.
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
     * obtained, then the in-memory data will be spilled to RSS servers.
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
        if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
            logger.info("Spilling data because number of spilledRecords crossed the threshold " + numElementsForSpillThreshold);
            spill();
        }

        Long startTime = System.nanoTime();
        growPointerArrayIfNecessary();
        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        // Need 4 or 8 bytes to store the record length.
        final int required = length + uaoSize;
        acquireNewPageIfNecessary(required);

        assert(currentPage != null);
        final Object base = currentPage.getBaseObject();
        final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
        UnsafeAlignedOffset.putSize(base, pageCursor, length);
        pageCursor += uaoSize;
        Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
        pageCursor += length;
        recordsRead += 1;
        inMemSorter.insertRecord(recordAddress, partitionId);
        totalSerializationTime += System.nanoTime() - startTime;
        // Track the max record size to decide buffer size
        if (length > maxRecordSize) {
            maxRecordSize = length;
        }
    }


    /**
     * Close the sorter, causing any buffered data to be sorted and sent to remote RSS servers.
     *
     * @return metadata for the spill files written by this sorter. If no records were ever inserted
     * into this sorter, then this will return an empty array.
     * @throws IOException
     */
    public Tuple2<ShuffleWriteMetadata, ShuffleWriteTimeMetadata> spillAndFreeMemory() throws
            IOException, RssMapperDataMismatchException {
        ShuffleWriteMetadata writeMetadata = null;
        ShuffleWriteTimeMetadata writeTimeMetadata = null;
        if (inMemSorter != null) {
            writeIterator();
            assert(recordsRead == recordsWritten);
            if (recordsRead != recordsWritten && enableRecordCountMatchCheck) {
                throw new RssMapperDataMismatchException(recordsRead, recordsWritten,
                        "");
            }
            writeMetadata = new ShuffleWriteMetadata(recordsRead, recordsWritten, curator.numberOfSpills(),
                    curator.partitionLengths(), 0, curator.numberOfNetworkWrites());
            writeTimeMetadata = new ShuffleWriteTimeMetadata(totalSerializationTime, curator.totalCompressionTime(),
                    curator.totalUploadTime(), curator.totalMemoryFetchTime(), curator.totalSendDataBlockTime());
            cleanupResources();
        }
        return new Tuple2(writeMetadata, writeTimeMetadata);
    }
}
