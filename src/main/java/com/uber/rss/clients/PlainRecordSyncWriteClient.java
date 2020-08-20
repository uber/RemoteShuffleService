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
  public void sendRecord(int partition, ByteBuffer key, ByteBuffer value) {
    int totalRecordBytes = getRecordSerializedSize(key, value);
    ByteBuf recordSerializedByteBuf = Unpooled.buffer(totalRecordBytes);
    writeRecordToBuffer(recordSerializedByteBuf, key, value);
    dataBlockSyncWriteClient.writeData(partition, shuffleMapTaskAttemptId.getTaskAttemptId(), recordSerializedByteBuf);
  }

  @Override
  public String toString() {
    return "PlainRecordSyncWriteClient{" +
        super.toString() +
        '}';
  }
}
