package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class RetryUtilsTest {

  @Test
  public void firstTrySucceeds() {
    String result = RetryUtils.retry(100, 100, 0, "test", () -> {
      return "ok";
    });
    Assert.assertEquals(result, "ok");
  }

  @Test
  public void multipleRetry() {
    int retryMaxMillis = 100;
    AtomicInteger counter = new AtomicInteger();
    long startTime = System.currentTimeMillis();
    RetryUtils.retry(0, 60000, retryMaxMillis, "test", () -> {
      int value = counter.incrementAndGet();
      if (System.currentTimeMillis() - startTime <= retryMaxMillis/2) {
        throw new RuntimeException("simulated exception");
      }
      return value;
    });
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testThrowException() {
    int retryMaxMillis = 100;
    AtomicInteger counter = new AtomicInteger();
    RetryUtils.retry(1, 10, retryMaxMillis, "test", () -> {
      counter.incrementAndGet();
      throw new RuntimeException("simulated exception");
    });
  }

}
