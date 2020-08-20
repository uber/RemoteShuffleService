package com.uber.rss.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MetricGroupContainer <K, M extends MetricGroup<K>> {
    private final Function<? super K, ? extends M> createFunction;
    
    private final ConcurrentHashMap<K, M> metricGroups = new ConcurrentHashMap<>();

    public MetricGroupContainer(Function<? super K, ? extends M> createFunction) {
        this.createFunction = createFunction;
    }

    public M getMetricGroup(K key) {
        return metricGroups.computeIfAbsent(key, createFunction);
    }

    public void removeMetricGroup(K key) {
        M metricGroup = metricGroups.remove(key);
        if (metricGroup != null) {
            metricGroup.close();
        }
    }
}
