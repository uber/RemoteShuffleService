/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
