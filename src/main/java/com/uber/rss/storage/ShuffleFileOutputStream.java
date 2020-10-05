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

import com.uber.rss.exceptions.RssException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.CountedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;

/***
 * Local file based shuffle output stream.
 */
public class ShuffleFileOutputStream implements ShuffleOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(ShuffleFileOutputStream.class);
    
    private final String filePath;
    private OutputStream outputStream;
    private long initialFileSize = 0L;
    private CountedOutputStream internalCountedOutputStream;
    private final FileDescriptor fileDescriptor;

    public ShuffleFileOutputStream(File file) {
        this.filePath = file.getAbsolutePath();
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            initialFileSize = fileOutputStream.getChannel().position();
            fileDescriptor = fileOutputStream.getFD();
            outputStream = fileOutputStream;
            internalCountedOutputStream = new CountedOutputStream(outputStream);
            outputStream = internalCountedOutputStream;
        } catch (Throwable e) {
            M3Stats.addException(e, M3Stats.TAG_VALUE_SHUFFLE_OUTPUT_STREAM);
            throw new RssException(
                    "Failed to open or create writable file: " + this.filePath, e);
        }
    }

    @Override
    public void write(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        
        try {
            outputStream.write(bytes);
        } catch (Throwable e) {
            throw new RuntimeException(
                    "Failed to write file: " + filePath 
                            + ", number of bytes: " + bytes.length, e);
        }
    }

    @Override
    public void flush() {
        try {
            outputStream.flush();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to flush file: " + filePath, e);
        }
    }

    @Override
    public void fsync() {
        try {
            flush();
            fileDescriptor.sync();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to fsync file: " + filePath, e);
        }
    }

    @Override
    public void close() {
        try {
            flush();
            outputStream.close();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to close file: " + filePath, e);
        }
    }

    @Override
    public String getLocation() {
        return filePath;
    }

    @Override
    public long getWrittenBytes() {
        return initialFileSize + internalCountedOutputStream.getWrittenBytes();
    }

    @Override
    public String toString() {
        return "ShuffleFileOutputStream{" +
                "filePath='" + filePath + '\'' +
                '}';
    }
}
