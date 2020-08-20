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

package com.uber.rss.storage;

/***
 * Shuffle output stream interface.
 */
public interface ShuffleOutputStream extends AutoCloseable {
    /***
     * Write data to the stream.
     * @param bytes
     */
    void write(byte[] bytes);

    /***
     * Flush the stream. This might only flush data 
     * into operation system's memory cache without
     * really flushing data to underlying device.
     */
    void flush();

    /***
     * Fsync the stream. Make sure data is written 
     * to underlying device. See this link for fsync:
     * http://man7.org/linux/man-pages/man2/fdatasync.2.html
     */
    void fsync();

    /***
     * Close the stream.
     */
    void close();

    /***
     * Get file location for this stream.
     * @return
     */
    String getLocation();

    long getWrittenBytes();
}
