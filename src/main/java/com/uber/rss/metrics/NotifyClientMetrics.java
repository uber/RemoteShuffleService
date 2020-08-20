package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;

import java.util.HashMap;
import java.util.Map;

public class NotifyClientMetrics extends MetricGroup<NotifyClientMetricsKey> {

    private final Counter numClients;
    
    public NotifyClientMetrics(NotifyClientMetricsKey key) {
        super(key);

        this.numClients = scope.counter("numClients");
    }

    public Counter getNumClients() {
        return numClients;
    }

    @Override
    protected Scope createScope(NotifyClientMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
        tags.put(M3Stats.TAG_NAME_USER, key.getUser());
        return M3Stats.createSubScope(tags);
    }
}
