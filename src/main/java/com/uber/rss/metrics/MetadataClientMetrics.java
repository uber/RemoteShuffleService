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

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class MetadataClientMetrics extends MetricGroup<MetadataClientMetricsKey> {

    private final Counter numRequests;
    private final Counter numFailures;
    private final Timer requestLatency;
    private final Gauge numConnections;

    public MetadataClientMetrics(MetadataClientMetricsKey key) {
        super(key);

        this.numRequests = scope.counter("numRequests");
        this.numFailures = scope.counter("numFailures");
        this.requestLatency = scope.timer("requestLatency");
        this.numConnections = scope.gauge("numConnections");
    }

    public Counter getNumRequests() {
        return numRequests;
    }

    public Counter getNumFailures() {
        return numFailures;
    }

    public Timer getRequestLatency() {
        return requestLatency;
    }

    public Gauge getNumConnections() {
        return numConnections;
    }

    @Override
    protected Scope createScope(MetadataClientMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_CLIENT, key.getClient());
        tags.put(M3Stats.TAG_NAME_OPERATION, key.getOperation());
        return M3Stats.createSubScope(tags);
    }
}
