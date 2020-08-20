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

package com.uber.rss.execution;

import com.uber.rss.messages.AppDeletionStateItem;
import com.uber.rss.messages.BaseMessage;
import com.uber.rss.messages.CommitMarkerStateItem;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.StageCorruptionStateItem;
import com.uber.rss.messages.StageInfoStateItem;
import com.uber.rss.messages.TaskAttemptCommitStateItem;
import com.uber.rss.util.ByteBufUtils;
import com.uber.rss.util.StreamUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class LocalFileStateStoreIterator implements Iterator<BaseMessage>, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LocalFileStateStoreIterator.class);

  private final List<String> files;

  private int nextFileIndex = 0;

  private String currentFile;
  private FileInputStream fileStream;


  private final List<BaseMessage> messages = new ArrayList<>();
  private int nextMessageIndex = 0;

  public LocalFileStateStoreIterator(Collection<String> files) {
    this.files = new ArrayList<>(files);
  }

  @Override
  public boolean hasNext() {
    readDataIfNecessary();
    return nextMessageIndex < messages.size();
  }

  @Override
  public BaseMessage next() {
    readDataIfNecessary();
    if (nextMessageIndex < messages.size()) {
      return messages.get(nextMessageIndex++);
    } else {
      return null;
    }
  }

  private void readDataIfNecessary() {
    if (nextMessageIndex < messages.size()) {
      return;
    }

    messages.clear();
    nextMessageIndex = 0;

    while (messages.isEmpty()) {
      openFileIfNecessary();
      if (fileStream == null) {
        return;
      }

      List<BaseMessage> nextBatch = readDataBatch();
      while (nextBatch.isEmpty()) {
        openFileIfNecessary();
        if (fileStream == null) {
          return;
        }

        nextBatch = readDataBatch();
      }

      messages.addAll(nextBatch);
    }
  }

  private void openFileIfNecessary() {
    while (fileStream == null) {
      if (nextFileIndex >= files.size()) {
        return;
      }

      currentFile = files.get(nextFileIndex++);

      try {
        logger.info(String.format("Opening state file: %s", currentFile));
        fileStream = new FileInputStream(currentFile);
      } catch (FileNotFoundException e) {
        logger.warn(String.format("Failed to open state file %s", currentFile), e);
        fileStream = null;
        continue;
      }
    }
  }

  // Read a batch of data: a sequence of data items right before next commit marker. Return empty list if
  // not hit next commit marker.
  private List<BaseMessage> readDataBatch() {
    List<BaseMessage> result = new ArrayList<>();
    BaseMessage item = readDataItem();
    while (item != null) {
      if (item instanceof CommitMarkerStateItem) {
        return result;
      }
      result.add(item);
      item = readDataItem();
    }
    return Collections.emptyList();
  }

  private BaseMessage readDataItem() {
    // read message type
    byte[] bytes = readBytes(Integer.BYTES);
    if (bytes == null) {
      closeCurrentFileStream();
      return null;
    }
    int messageType = ByteBufUtils.readInt(bytes, 0);
    // read length
    bytes = readBytes(Integer.BYTES);
    if (bytes == null) {
      logger.warn(String.format("Failed to read length field in state file %s", currentFile));
      closeCurrentFileStream();
      return null;
    }
    int length = ByteBufUtils.readInt(bytes, 0);
    if (length < 0) {
      logger.warn(String.format("Hit invalid length field %s in state file %s", length, currentFile));
      closeCurrentFileStream();
      return null;
    }
    // read bytes after length
    bytes = readBytes(length);
    if (bytes == null) {
      logger.warn(String.format("Failed to read payload field in state file %s", currentFile));
      closeCurrentFileStream();
      return null;
    }

    try {
      ByteBuf buf = Unpooled.wrappedBuffer(bytes);
      switch (messageType) {
        case MessageConstants.MESSAGE_StageInfoStateItem:
          return StageInfoStateItem.deserialize(buf);
        case MessageConstants.MESSAGE_TaskAttemptCommitStateItem:
          return TaskAttemptCommitStateItem.deserialize(buf);
        case MessageConstants.MESSAGE_AppDeletionStateItem:
          return AppDeletionStateItem.deserialize(buf);
        case MessageConstants.MESSAGE_CommitMarkerStateItem:
          return CommitMarkerStateItem.deserialize(buf);
        case MessageConstants.MESSAGE_StageCorruptionStateItem:
          return StageCorruptionStateItem.deserialize(buf);
        default:
          logger.warn(String.format("Hit unsupported message type %s in state file %s", messageType, currentFile));
          closeCurrentFileStream();
          return null;
      }
    } catch (Throwable ex) {
      logger.warn(String.format("Failed to deserialize message type %s from state file: %s", messageType, currentFile), ex);
      closeCurrentFileStream();
      return null;
    }
  }

  private byte[] readBytes(int numBytes) {
    try {
      long position = fileStream.getChannel().position();
      byte[] bytes = StreamUtils.readBytes(fileStream, numBytes);
      if (bytes == null) {
        logger.info(String.format("Finished reading state file %s after reading %s bytes", currentFile, position));
        return null;
      } else if (bytes.length < numBytes) {
        logger.warn(String.format("Hit corrupted state file %s after reading %s bytes", currentFile, position));
        return null;
      } else {
        return bytes;
      }
    } catch (Throwable e) {
      logger.warn(String.format("Failed to read state file %s", currentFile), e);
      return null;
    }
  }

  private void closeCurrentFileStream() {
    if (fileStream == null) {
      return;
    }

    try {
      logger.info(String.format("Closing state file: %s", currentFile));
      fileStream.close();
    } catch (IOException e) {
      logger.warn(String.format("Failed to close state file: %s", currentFile), e);
    }
    fileStream = null;
  }

  @Override
  public void close() {
    closeCurrentFileStream();
  }
}
