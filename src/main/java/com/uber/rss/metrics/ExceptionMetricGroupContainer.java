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

public class ExceptionMetricGroupContainer  {
    private MetricGroupContainer<ExceptionMetricsKey, ExceptionMetrics> metricGroupContainer;
    
    public ExceptionMetricGroupContainer() {
        this.metricGroupContainer = new MetricGroupContainer<>(
                t->new ExceptionMetrics(t.getExceptionName(), t.getExceptionSource()));
    }

    public ExceptionMetrics getMetricGroup(Throwable ex, String exceptionSource) {
        ExceptionMetricsKey tag = new ExceptionMetricsKey(ex.getClass().getSimpleName(), exceptionSource);
        return metricGroupContainer.getMetricGroup(tag);
    }
}
