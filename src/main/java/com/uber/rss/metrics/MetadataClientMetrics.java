package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class MetadataClientMetrics extends MetricGroup<MetadataClientMetricsKey> {

    private final Counter numRequests;
    private final Counter numFailures;
    private final Timer requestLatency;
    
    public MetadataClientMetrics(MetadataClientMetricsKey key) {
        super(key);

        this.numRequests = scope.counter("numRequests");
        this.numFailures = scope.counter("numFailures");
        this.requestLatency = scope.timer("requestLatency");
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

    @Override
    protected Scope createScope(MetadataClientMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_CLIENT, key.getClient());
        tags.put(M3Stats.TAG_NAME_OPERATION, key.getOperation());
        return M3Stats.createSubScope(tags);
    }
}
