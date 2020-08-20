package com.uber.rss.metrics;

import java.util.Objects;

public class ExceptionMetricsKey {
    private final String exceptionName;
    private final String exceptionSource;

    public ExceptionMetricsKey(String exceptionName, String exceptionSource) {
        this.exceptionName = exceptionName;
        this.exceptionSource = exceptionSource;
    }

    public String getExceptionName() {
        return exceptionName;
    }

    public String getExceptionSource() {
        return exceptionSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExceptionMetricsKey that = (ExceptionMetricsKey) o;
        return Objects.equals(exceptionName, that.exceptionName) &&
                Objects.equals(exceptionSource, that.exceptionSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exceptionName, exceptionSource);
    }

    @Override
    public String toString() {
        return "ExceptionMetricsKey{" +
                "exceptionName='" + exceptionName + '\'' +
                ", exceptionSource='" + exceptionSource + '\'' +
                '}';
    }
}
