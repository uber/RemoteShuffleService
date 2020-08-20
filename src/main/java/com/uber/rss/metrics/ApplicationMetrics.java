package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;

import java.util.HashMap;
import java.util.Map;

public class ApplicationMetrics extends MetricGroup<ApplicationMetricsKey> {

    private final Counter numApplications;
    
    public ApplicationMetrics(ApplicationMetricsKey key) {
        super(key);

        this.numApplications = scope.counter("numApplications");
    }

    public Counter getNumApplications() {
        return numApplications;
    }

    @Override
    protected Scope createScope(ApplicationMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_USER, key.getUser());
        tags.put(M3Stats.TAG_NAME_ATTEMPT_ID, key.getAttemptId());
        return M3Stats.createSubScope(tags);
    }
}
