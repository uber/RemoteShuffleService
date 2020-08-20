package com.uber.rss.metrics;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ExceptionMetrics extends MetricGroup<ExceptionMetricsKey> {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionMetrics.class);
    
    private final Counter numExceptions;
    
    public ExceptionMetrics(String exceptionName, String exceptionSource) {
        super(new ExceptionMetricsKey(exceptionName, exceptionSource));
        this.numExceptions = scope.counter("numExceptions");
    }

    public Counter getNumExceptions() {
        return numExceptions;
    }

    @Override
    public void close() {
        M3Stats.decreaseNumM3Scopes();
    }

    @Override
    protected Scope createScope(ExceptionMetricsKey key) {
        Map<String, String> tags = new HashMap<>();
        tags.put("exception", key.getExceptionName());
        tags.put("source", key.getExceptionSource());
        return M3Stats.createSubScope(tags);
    }
}
