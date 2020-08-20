package com.uber.rss.metrics;

import java.util.function.Function;

public class NettyServerSideMetricGroupContainer <M extends MetricGroup<NettyServerSideMetricsKey>>  {
    private MetricGroupContainer<NettyServerSideMetricsKey, M> metricGroupContainer;
    
    public NettyServerSideMetricGroupContainer(Function<NettyServerSideMetricsKey, ? extends M> createFunction) {
        this.metricGroupContainer = new MetricGroupContainer<NettyServerSideMetricsKey, M>(createFunction);
    }

    public M getMetricGroup(String user) {
        return metricGroupContainer.getMetricGroup(new NettyServerSideMetricsKey(user));
    }

    public void removeMetricGroup(String user) {
        metricGroupContainer.removeMetricGroup(new NettyServerSideMetricsKey(user));
    }
}
