package com.uber.rss.handlers;

import com.uber.rss.common.AppTaskAttemptId;
import io.netty.channel.Channel;

public class UploadChannel {
    private AppTaskAttemptId appTaskAttemptId;
    private Channel channel;
    private volatile long lastStopTime = 0;

    public UploadChannel(AppTaskAttemptId appTaskAttemptId, Channel channel) {
        this.appTaskAttemptId = appTaskAttemptId;
        this.channel = channel;
    }

    public AppTaskAttemptId getAppTaskAttemptId() {
        return appTaskAttemptId;
    }

    public void setAutoRead(boolean autoRead) {
        channel.config().setAutoRead(autoRead);
        if (!autoRead) {
            lastStopTime = System.currentTimeMillis();
        }
    }
    
    public boolean isAutoRead() {
        return channel.config().isAutoRead();
    }
    
    public boolean needToRenableAutoRead(long maxPauseMillis) {
        return !channel.config().isAutoRead() 
                && System.currentTimeMillis() - lastStopTime > maxPauseMillis;
    }
}
