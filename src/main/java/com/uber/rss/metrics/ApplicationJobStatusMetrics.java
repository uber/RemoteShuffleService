package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;

import java.util.HashMap;
import java.util.Map;

public class ApplicationJobStatusMetrics extends MetricGroup<ApplicationJobStatusMetricsKey> {

    private final Counter numApplicationJobs;
    private final Counter numFailedJobs;
    
    public ApplicationJobStatusMetrics(ApplicationJobStatusMetricsKey key) {
        super(key);

        this.numApplicationJobs = scope.counter("numApplicationJobs2");
        this.numFailedJobs = scope.counter("numFailedJobs");
    }

    public Counter getNumApplicationJobs() {
        return numApplicationJobs;
    }

    public Counter getNumFailedJobs() {
        return numFailedJobs;
    }

    @Override
    protected Scope createScope(ApplicationJobStatusMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_USER, key.getUser());
        tags.put(M3Stats.TAG_NAME_JOB_STATUS, key.getJobStatus());

        return M3Stats.createSubScope(tags);
    }
}
