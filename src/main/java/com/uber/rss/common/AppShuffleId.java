package com.uber.rss.common;

import java.util.Objects;

/***
 * Fully qualified ID for application shuffle stage.
 */
public class AppShuffleId {
    private final String appId;
    private final String appAttempt;
    private final int shuffleId;

    public AppShuffleId(String appId, String appAttempt, int shuffleId) {
        this.appId = appId;
        this.appAttempt = appAttempt;
        this.shuffleId = shuffleId;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppAttempt() {
        return appAttempt;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppShuffleId that = (AppShuffleId) o;
        return shuffleId == that.shuffleId &&
                Objects.equals(appId, that.appId) &&
                Objects.equals(appAttempt, that.appAttempt);
    }

    @Override
    public int hashCode() {

        return Objects.hash(appId, appAttempt, shuffleId);
    }

    @Override
    public String toString() {
        return "AppShuffleId{" +
                "appId='" + appId + '\'' +
                ", appAttempt='" + appAttempt + '\'' +
                ", shuffleId=" + shuffleId +
                '}';
    }
}
