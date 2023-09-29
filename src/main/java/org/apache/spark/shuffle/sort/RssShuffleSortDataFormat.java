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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.SortDataFormat;

/**
 * Borrowed from Spark code base(Spark 2.4.3). It could not be referred directly from RSS because it is package private
 * in Spark and native spark classes and classes in jars externally passed are loaded by different class loader.
 */
final class RssShuffleSortDataFormat extends SortDataFormat<RssPackedRecordPointer, LongArray> {

    private final LongArray buffer;

    RssShuffleSortDataFormat(LongArray buffer) {
        this.buffer = buffer;
    }

    @Override
    public RssPackedRecordPointer getKey(LongArray data, int pos) {
        // Since we re-use keys, this method shouldn't be called.
        throw new UnsupportedOperationException();
    }

    @Override
    public RssPackedRecordPointer newKey() {
        return new RssPackedRecordPointer();
    }

    @Override
    public RssPackedRecordPointer getKey(LongArray data, int pos, RssPackedRecordPointer reuse) {
        reuse.set(data.get(pos));
        return reuse;
    }

    @Override
    public void swap(LongArray data, int pos0, int pos1) {
        final long temp = data.get(pos0);
        data.set(pos0, data.get(pos1));
        data.set(pos1, temp);
    }

    @Override
    public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
        dst.set(dstPos, src.get(srcPos));
    }

    @Override
    public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
        Platform.copyMemory(
                src.getBaseObject(),
                src.getBaseOffset() + srcPos * 8L,
                dst.getBaseObject(),
                dst.getBaseOffset() + dstPos * 8L,
                length * 8L
        );
    }

    @Override
    public LongArray allocate(int length) {
        assert (length <= buffer.size()) :
                "the buffer is smaller than required: " + buffer.size() + " < " + length;
        return buffer;
    }
}
