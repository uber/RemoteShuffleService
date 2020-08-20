package com.uber.rss.execution;

import java.util.concurrent.atomic.AtomicLong;

/***
 * This class contains the state for each application.
 */
public class ExecutorAppState {
    private final String appId;

    private final AtomicLong numWriteBytes = new AtomicLong();
    
    // The timestamp (milliseconds) to indicate the liveness of the shuffle stage
    private final AtomicLong livenessTimestamp = new AtomicLong(System.currentTimeMillis());

    public ExecutorAppState(String appId) {
        this.appId = appId;
    }

    public final String getAppId() {
        return appId;
    }

    public final void updateLivenessTimestamp() {
        livenessTimestamp.set(System.currentTimeMillis());
    }
    
    public final long getLivenessTimestamp() {
        return livenessTimestamp.get();
    }

    public final long addNumWriteBytes(long delta) {
        return numWriteBytes.addAndGet(delta);
    }

    public final long getNumWriteBytes() {
        return numWriteBytes.get();
    }

    @Override
    public String toString() {
        return "ExecutorAppState{" +
                "appId='" + appId + '\'' +
                ", numWriteBytes=" + numWriteBytes.get() +
                ", livenessTimestamp=" + livenessTimestamp +
                '}';
    }
}
