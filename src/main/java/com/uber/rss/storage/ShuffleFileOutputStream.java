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

import com.uber.rss.metrics.M3Stats;
import com.uber.rss.exceptions.RssException;
import com.uber.rss.exceptions.RssFileCorruptedException;
import com.uber.rss.util.CountedOutputStream;
import com.uber.rss.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

  public ShuffleFileOutputStream(File file) {
    this.filePath = file.getAbsolutePath();
    try {
      FileOutputStream fileOutputStream = new FileOutputStream(file, true);
      initialFileSize = fileOutputStream.getChannel().position();
      outputStream = fileOutputStream;
      internalCountedOutputStream = new CountedOutputStream(outputStream);
      outputStream = internalCountedOutputStream;
    } catch (Throwable e) {
      M3Stats.addException(e, this.getClass().getSimpleName());
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
      throw new RssFileCorruptedException(String.format(
          "Failed to write %s bytes to file %s with exception %s",
          bytes.length, filePath, ExceptionUtils.getSimpleMessage(e)),
          e);
    }
  }

  @Override
  public void close() {
    try {
      outputStream.close();
    } catch (Throwable e) {
      throw new RssFileCorruptedException(String.format(
          "Failed to close file %s with exception %s",
          filePath, ExceptionUtils.getSimpleMessage(e)),
          e);
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
