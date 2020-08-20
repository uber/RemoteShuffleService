package com.uber.rss.metrics;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.StopwatchRecorder;
import com.uber.m3.tally.Timer;
import com.uber.m3.util.Duration;

import java.util.Map;

public class M3DummyScope implements Scope {
  @Override
  public Counter counter(String s) {
    return new Counter() {
      @Override
      public void inc(long l) {
      }
    };
  }

  @Override
  public Gauge gauge(String s) {
    return new Gauge() {
      @Override
      public void update(double v) {
      }
    };
  }

  @Override
  public Timer timer(String s) {
    return new Timer() {
      @Override
      public void record(Duration duration) {
      }

      @Override
      public Stopwatch start() {
        StopwatchRecorder stopwatchRecorder = new StopwatchRecorder() {
          @Override
          public void recordStopwatch(long l) {
          }
        };
        return new Stopwatch(System.nanoTime(), stopwatchRecorder);
      }
    };
  }

  @Override
  public Histogram histogram(String s, Buckets buckets) {
    return new Histogram() {
      @Override
      public void recordValue(double v) {
      }

      @Override
      public void recordDuration(Duration duration) {
      }

      @Override
      public Stopwatch start() {
        StopwatchRecorder stopwatchRecorder = new StopwatchRecorder() {
          @Override
          public void recordStopwatch(long l) {
          }
        };
        return new Stopwatch(System.nanoTime(), stopwatchRecorder);
      }
    };
  }

  @Override
  public Scope tagged(Map<String, String> map) {
    return this;
  }

  @Override
  public Scope subScope(String s) {
    return this;
  }

  @Override
  public Capabilities capabilities() {
    return new Capabilities() {
      @Override
      public boolean reporting() {
        return false;
      }

      @Override
      public boolean tagging() {
        return false;
      }
    };
  }

  @Override
  public void close() {
  }
}
