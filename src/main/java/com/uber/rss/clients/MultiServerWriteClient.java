package com.uber.rss.clients;

/***
 * Shuffle write client to upload data (records) to multiple shuffle servers.
 */
public interface MultiServerWriteClient extends RecordWriter {

  void connect();

}
