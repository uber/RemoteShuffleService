package com.uber.rss.metrics;

import java.util.Objects;

public class ApplicationJobStatusMetricsKey {
    private String user;
    private String jobStatus;

    public ApplicationJobStatusMetricsKey(String user, String jobStatus) {
        this.user = user;
        this.jobStatus = jobStatus;
    }

    public String getUser() {
        return user;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplicationJobStatusMetricsKey that = (ApplicationJobStatusMetricsKey) o;
        return Objects.equals(user, that.user) &&
                Objects.equals(jobStatus, that.jobStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, jobStatus);
    }

    @Override
    public String toString() {
        return "ApplicationJobStatusMetricsKey{" +
                "user='" + user + '\'' +
                ", jobStatus='" + jobStatus + '\'' +
                '}';
    }
}
