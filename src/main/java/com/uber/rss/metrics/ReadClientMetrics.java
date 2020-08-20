package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class ReadClientMetrics extends MetricGroup<ReadClientMetricsKey> {

    private final Counter numIgnoredBlocks;
    private final Counter numReadBytes;
    private final Timer readConnectLatency;
    private final Timer reducerWaitTime;

    private final Gauge bufferSize;
    
    public ReadClientMetrics(ReadClientMetricsKey key) {
        super(key);

        // The name like "numClients" was used when there were a lot of metric series which caused M3 issue, e.g.
        // not able to load the values in dashboard. Use new names ending with a number suffix to create new metrics in M3.
        this.numIgnoredBlocks = scope.counter("numIgnoredBlocks4");
        this.numReadBytes = scope.counter("numReadBytes4");
        this.readConnectLatency = scope.timer("readConnectLatency4");
        this.reducerWaitTime = scope.timer("reducerWaitTime4");
        this.bufferSize = scope.gauge("bufferSize4");
    }

    public Counter getNumIgnoredBlocks() {
        return numIgnoredBlocks;
    }

    public Counter getNumReadBytes() {
        return numReadBytes;
    }

    public Timer getReadConnectLatency() {
        return readConnectLatency;
    }

    public Timer getReducerWaitTime() {
        return reducerWaitTime;
    }

    public Gauge getBufferSize() {
        return bufferSize;
    }

    @Override
    protected Scope createScope(ReadClientMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
        tags.put(M3Stats.TAG_NAME_USER, key.getUser());
        return M3Stats.createSubScope(tags);
    }
}
