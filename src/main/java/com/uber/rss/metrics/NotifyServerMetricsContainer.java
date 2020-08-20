package com.uber.rss.metrics;

public class NotifyServerMetricsContainer {
    private MetricGroupContainer<ApplicationJobStatusMetricsKey, ApplicationJobStatusMetrics> applicationJobStatusMetricsContainer;

    private MetricGroupContainer<ApplicationMetricsKey, ApplicationMetrics> applicationMetricsContainer;
    
    public NotifyServerMetricsContainer() {
        this.applicationJobStatusMetricsContainer = new MetricGroupContainer<>(
                t->new ApplicationJobStatusMetrics(t));
        
        this.applicationMetricsContainer = new MetricGroupContainer<>(
                t->new ApplicationMetrics(t));
    }
    
    public ApplicationJobStatusMetrics getApplicationJobStatusMetrics(String user, String jobStatus) {
        ApplicationJobStatusMetricsKey key = new ApplicationJobStatusMetricsKey(user, jobStatus);
        return applicationJobStatusMetricsContainer.getMetricGroup(key);
    }

    public ApplicationMetrics getApplicationMetrics(String user, String attemptId) {
        ApplicationMetricsKey key = new ApplicationMetricsKey(user, attemptId);
        return applicationMetricsContainer.getMetricGroup(key);
    }
}
