package com.uber.rss.metrics;

import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;

import java.util.HashMap;
import java.util.Map;

public class ClientConnectMetrics extends MetricGroup<ClientConnectMetricsKey> {

    private final Timer socketConnectLatency;
    private final Gauge socketConnectRetries;

    public ClientConnectMetrics(ClientConnectMetricsKey key) {
        super(key);

        this.socketConnectLatency = scope.timer("socketConnectLatency");
        this.socketConnectRetries = scope.gauge("socketConnectRetries");
    }

    public Timer getSocketConnectLatency() {
        return socketConnectLatency;
    }

    public Gauge getSocketConnectRetries() {
        return socketConnectRetries;
    }

    @Override
    protected Scope createScope(ClientConnectMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put(M3Stats.TAG_NAME_SOURCE, key.getSource());
        tags.put(M3Stats.TAG_NAME_REMOTE, key.getRemote());
        return M3Stats.createSubScope(tags);
    }
}
