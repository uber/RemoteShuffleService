package com.uber.rss.metrics;

public class MetadataClientMetricsContainer {
    private MetricGroupContainer<MetadataClientMetricsKey, MetadataClientMetrics> metricGroupContainer;
    
    public MetadataClientMetricsContainer() {
        this.metricGroupContainer = new MetricGroupContainer<>(
                t->new MetadataClientMetrics(t));
    }

    public MetadataClientMetrics getMetricGroup(String client, String operation) {
        MetadataClientMetricsKey tag = new MetadataClientMetricsKey(client, operation);
        return metricGroupContainer.getMetricGroup(tag);
    }
}
