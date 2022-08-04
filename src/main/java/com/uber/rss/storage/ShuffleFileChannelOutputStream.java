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

package com.uber.rss.storage;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssFileCorruptedException;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.ExceptionUtils;

import io.netty.buffer.ByteBuf;

/***
 * Local file based shuffle output stream.
 */
public class ShuffleFileChannelOutputStream implements ShuffleOutputStream {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleFileChannelOutputStream.class);

  private final String filePath;

  private FileChannel channel;
  private RandomAccessFile raf;
  private long bytesWritten;

  public ShuffleFileChannelOutputStream(File file) {
    this.filePath = file.getAbsolutePath();
    try {
      raf = new RandomAccessFile(file, "rw");
      channel = raf.getChannel();
      channel.position(raf.length());
      bytesWritten = channel.position();
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
      throw new RssException("Failed to open or create writable file: " + this.filePath, e);
    }
  }

  @Override
  public int write(ByteBuf bytes) {
    if (bytes == null || bytes.readableBytes() == 0) {
      return 0;
    }

    try {
      int bytesToRead = bytes.readableBytes();
      int readBytes = 0;
      while (readBytes < bytesToRead) {
        int transferedBytes = bytes.readBytes(channel, bytesWritten,
            bytesToRead - readBytes);
        bytesWritten += transferedBytes;
        readBytes += transferedBytes;
      }
      return readBytes;
    } catch (Throwable e) {
      throw new RssFileCorruptedException(
          String.format("Failed to write %s bytes to file %s with exception %s",
              bytes.readableBytes(), filePath, ExceptionUtils.getSimpleMessage(e)),
          e);
    }
  }

  @Override
  public void close() {
    try {
      channel.force(true);
      channel.close();
      raf.close();
    } catch (Throwable e) {
      throw new RssFileCorruptedException(String.format("Failed to close file %s with exception %s",
          filePath, ExceptionUtils.getSimpleMessage(e)), e);
    }
  }

  @Override
  public String getLocation() {
    return filePath;
  }

  @Override
  public long getWrittenBytes() {
    return bytesWritten;
  }

  @Override
  public String toString() {
    return "ShuffleFileOutputStream{" + "filePath='" + filePath + '\'' + '}';
  }
}
