package com.uber.rss.testutil;

import com.uber.rss.clients.ShuffleWriteConfig;

public class TestConstants {
  public static final int NETWORK_TIMEOUT = 30000;

  public static final int DATA_AVAILABLE_POLL_INTERVAL = 10;
  public static final int DATA_AVAILABLE_TIMEOUT = 30000;

  public static final int COMPRESSION_BUFFER_SIZE = 64*1024;

  public static final ShuffleWriteConfig SHUFFLE_WRITE_CONFIG = new ShuffleWriteConfig("", (short)3);

  public static final long CONNECTION_IDLE_TIMEOUT_MILLIS = 30 * 1000;
}
