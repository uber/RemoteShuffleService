package com.uber.rss.metrics;

import java.util.Objects;

public class ApplicationMetricsKey {
    private String user;
    private String attemptId;

    public ApplicationMetricsKey(String user, String attemptId) {
        this.user = user;
        this.attemptId = attemptId;
    }

    public String getUser() {
        return user;
    }

    public String getAttemptId() {
        return attemptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplicationMetricsKey that = (ApplicationMetricsKey) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(attemptId, that.attemptId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(user, attemptId);
    }

    @Override
    public String toString() {
        return "ApplicationMetricsKey{" +
                "user='" + user + '\'' +
                ", attemptId='" + attemptId + '\'' +
                '}';
    }
}
