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
import java.io.*;

import com.uber.rss.exceptions.RssUnsafeShuffleWriteException;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.shuffle.rss.RssShuffleWriteManager;
import org.apache.spark.shuffle.rss.ShuffleWriteCurator;
import org.apache.spark.shuffle.rss.ShuffleWriteMetadata;
import org.apache.spark.shuffle.rss.ShuffleWriteTimeMetadata;

import scala.*;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.unsafe.Platform;

@Private
public class RssUnsafeShuffleWriter<K, V, C> extends RssShuffleWriteManager<K, V, C> {

    private static final Logger logger = LoggerFactory.getLogger(RssUnsafeShuffleWriter.class);

    private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

    @VisibleForTesting
    static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
    static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

    private final ShuffleWriteCurator curator;
    private final TaskMemoryManager memoryManager;
    private final SerializerInstance serializer;
    private final Partitioner partitioner;
    private final TaskMetrics taskMetrics;
    private final SparkConf sparkConf;
    private final int initialSortBufferSize;

    @Nullable private RssShuffleExternalSorter sorter;
    private long peakMemoryUsedBytes = 0;

    /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
    private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
        MyByteArrayOutputStream(int size) { super(size); }
        public byte[] getBuf() { return buf; }
    }

    private MyByteArrayOutputStream serBuffer;
    private SerializationStream serOutputStream;

    Tuple2<ShuffleWriteMetadata, ShuffleWriteTimeMetadata> metadata = null;

    /**
     * Are we in the process of stopping? Because map tasks can call stop() with success = true
     * and then call stop() with success = false if they get an exception, we want to make sure
     * we don't try to do clean up twice.
     */
    private boolean stopping = false;

    public RssUnsafeShuffleWriter(
            ShuffleWriteCurator curator,
            TaskMemoryManager memoryManager,
            ShuffleDependency<K, V, C> dependency,
            SparkConf sparkConf,
            TaskMetrics taskMetrics) throws IOException {
        super(curator);
        final int numPartitions = dependency.partitioner().numPartitions();
        if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
            throw new IllegalArgumentException(
                    "UnsafeShuffleWriter can only be used for shuffles with at most " +
                            SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
                            " reduce partitions");
        }
        this.curator = curator;
        this.memoryManager = memoryManager;
        this.serializer = dependency.serializer().newInstance();
        this.partitioner = dependency.partitioner();
        this.sparkConf = sparkConf;
        this.taskMetrics = taskMetrics;
        this.initialSortBufferSize = sparkConf.getInt("spark.shuffle.sort.initialBufferSize",
                DEFAULT_INITIAL_SORT_BUFFER_SIZE);
        open();
    }

    private void updatePeakMemoryUsed() {
        // sorter can be null if this writer is closed
        if (sorter != null) {
            long mem = sorter.getPeakMemoryUsedBytes();
            if (mem > peakMemoryUsedBytes) {
                peakMemoryUsedBytes = mem;
            }
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    @Override
    public int collectionSizeInBytes() {
        if (sorter != null) {
            // This mean the sorter was not closed correctly. Return non zero size so that, this task can be
            // failed
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public ShuffleWriteTimeMetadata getShuffleWriteTimeMetadata() {
        return metadata._2;
    }

    @Override
    public ShuffleWriteMetadata getShuffleWriteMetadata() {
        return metadata._1;
    }

    @Override
    public void addRecord(int partitionId, Product2<K, V> record) {
        // Keep track of success so we know if we encountered an exception
        // We do this rather than a standard try/catch/re-throw to handle
        // generic throwables.
        try {
            insertRecordIntoSorter(partitionId, record);
        } catch (Exception e) {
            logger.error("Exception while adding a new record", e);
            if (sorter != null) {
                sorter.cleanupResources();
            }
            throw new RssUnsafeShuffleWriteException(e.getMessage());
        }
    }

    private void open() {
        assert (sorter == null);
        sorter = new RssShuffleExternalSorter(
                memoryManager,
                taskMetrics,
                initialSortBufferSize,
                sparkConf,
                curator);
        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);
    }

    @VisibleForTesting
    void insertRecordIntoSorter(int partitionId, Product2<K, V> record) throws IOException {
        assert(sorter != null);
        final K key = record._1();
        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();

        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);

        sorter.insertRecord(serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
    }

    @Override
    public void stop(boolean success) {
        try {
            taskMetrics.incPeakExecutionMemory(getPeakMemoryUsedBytes());
            if (!stopping) {
                stopping = true;
            }
        } finally {
            if (sorter != null) {
                // If sorter is non-null, then this implies that we called stop() in response to an error,
                // so we need to clean up memory
                sorter.cleanupResources();
                sorter = null;
            }
            super.stop(success);
        }
    }

    @Override
    public void finish() {
        try {
            assert(sorter != null);
            updatePeakMemoryUsed();
            taskMetrics.incPeakExecutionMemory(getPeakMemoryUsedBytes());
            serBuffer = null;
            serOutputStream = null;
            metadata = sorter.spillAndFreeMemory();
            sorter = null;
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if (sorter != null) {
                sorter.cleanupResources();
            }
        }
    }

    @VisibleForTesting
    public void forceSorterToSpill() throws IOException {
        assert (sorter != null);
        sorter.spill();
    }
}
