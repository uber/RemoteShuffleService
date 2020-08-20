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
