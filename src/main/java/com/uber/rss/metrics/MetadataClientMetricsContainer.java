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

public class MetadataClientMetricsContainer {
    private final MetricGroupContainer<MetadataClientMetricsKey, MetadataClientMetrics> metricGroupContainer;
    
    public MetadataClientMetricsContainer() {
        this.metricGroupContainer = new MetricGroupContainer<>(
                t->new MetadataClientMetrics(t));
    }

    public MetadataClientMetrics getMetricGroup(String client, String operation) {
        MetadataClientMetricsKey tag = new MetadataClientMetricsKey(client, operation);
        return metricGroupContainer.getMetricGroup(tag);
    }
}
