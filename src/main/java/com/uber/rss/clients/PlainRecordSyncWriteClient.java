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

package com.uber.rss.clients;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/***
 * Shuffle write client to upload data (uncompressed records) to shuffle server.
 */
public class PlainRecordSyncWriteClient extends RecordSyncWriteClientBase {
  private static final Logger logger =
      LoggerFactory.getLogger(PlainRecordSyncWriteClient.class);

  public PlainRecordSyncWriteClient(String host, int port, int timeoutMillis, boolean finishUploadAck, String user, String appId, String appAttempt, ShuffleWriteConfig shuffleWriteConfig) {
    super(host, port, timeoutMillis, finishUploadAck, user, appId, appAttempt, shuffleWriteConfig);
  }

  @Override
  public void sendRecord(int partition, ByteBuffer value) {
    int totalRecordBytes = getRecordSerializedSize(value);
    ByteBuf recordSerializedByteBuf = Unpooled.buffer(totalRecordBytes);
    writeRecordToBuffer(recordSerializedByteBuf, value);
    dataBlockSyncWriteClient.writeData(partition, shuffleMapTaskAttemptId.getTaskAttemptId(), recordSerializedByteBuf);
  }

  @Override
  public String toString() {
    return "PlainRecordSyncWriteClient{" +
        super.toString() +
        '}';
  }
}
