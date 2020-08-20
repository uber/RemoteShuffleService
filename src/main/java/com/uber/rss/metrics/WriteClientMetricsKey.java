package com.uber.rss.metrics;

import java.util.Objects;

public class WriteClientMetricsKey {
    private String source;
    private String user;

    public WriteClientMetricsKey(String source, String user) {
        this.source = source;
        this.user = user;
    }

    public String getSource() {
        return source;
    }

    public String getUser() {
        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WriteClientMetricsKey that = (WriteClientMetricsKey) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, user);
    }

    @Override
    public String toString() {
        return "WriteClientMetricsKey{" +
                "source='" + source + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}
