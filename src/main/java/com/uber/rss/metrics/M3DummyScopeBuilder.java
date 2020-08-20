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
