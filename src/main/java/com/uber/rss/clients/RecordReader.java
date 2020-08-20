package com.uber.rss.clients;

import com.uber.rss.clients.RecordKeyValuePair;

/***
 * Shuffle record reader.
 */
public interface RecordReader extends AutoCloseable {

  RecordKeyValuePair readRecord();

  long getShuffleReadBytes();

  void close();
}
