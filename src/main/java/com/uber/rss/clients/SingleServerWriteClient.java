package com.uber.rss.clients;

import com.uber.rss.messages.ConnectUploadResponse;

/***
 * Shuffle write client to upload data (records) to shuffle server.
 */
public interface SingleServerWriteClient extends RecordWriter {

  ConnectUploadResponse connect();

}
