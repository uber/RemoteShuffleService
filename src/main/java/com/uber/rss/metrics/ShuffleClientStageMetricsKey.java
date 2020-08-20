package com.uber.rss.metrics;

import java.util.Objects;

public class ShuffleClientStageMetricsKey {
    private String user;
    private String queue;

    public ShuffleClientStageMetricsKey(String user, String queue) {
        this.user = user;
        this.queue = queue;
    }

    public String getUser() {
        return user;
    }

    public String getQueue() {
        return queue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShuffleClientStageMetricsKey that = (ShuffleClientStageMetricsKey) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(queue, that.queue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, queue);
    }

    @Override
    public String toString() {
        return "ShuffleClientStageMetricsKey{" +
                "user='" + user + '\'' +
                ", queue='" + queue + '\'' +
                '}';
    }
}
