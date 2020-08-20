package com.uber.rss.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(ThreadUtils.class);

  public static final long SHORT_WAIT_TIME = 500;

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      logger.info("Interrupted during sleeping", e);
    }
  }
}
