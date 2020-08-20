package com.uber.rss.metrics;

import com.uber.m3.tally.Scope;

public abstract class MetricGroup <K> implements AutoCloseable {
    protected final K key;
    protected final Scope scope;

    public MetricGroup(K key) {
        this.key = key;
        this.scope = createScope(key);
    }

    abstract protected Scope createScope(K key);
    
    @Override
    public void close() {
        M3Stats.decreaseNumM3Scopes();
    }
}
