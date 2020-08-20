package com.uber.rss.metrics;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.ScopeBuilder;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;

import java.util.Map;

public class M3DummyScopeBuilder extends RootScopeBuilder {
  @Override
  public ScopeBuilder reporter(StatsReporter reporter) {
    return this;
  }

  @Override
  public ScopeBuilder prefix(String prefix) {
    return this;
  }

  @Override
  public ScopeBuilder separator(String separator) {
    return this;
  }

  @Override
  public ScopeBuilder tags(Map<String, String> tags) {
    return this;
  }

  @Override
  public ScopeBuilder tags(ImmutableMap<String, String> tags) {
    return this;
  }

  @Override
  public ScopeBuilder defaultBuckets(Buckets defaultBuckets) {
    return this;
  }

  @Override
  public Scope reportEvery(Duration interval) {
    return new M3DummyScope();
  }
}
