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

package com.uber.rss.messages;

import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

/**
 * Notifies RSS when a Spark Stage is completed.
 */
public class FinishApplicationStageRequestMessage extends ControlMessage {
    private final String appId;
    private final String appAttempt;
    private final int stageId;

    public FinishApplicationStageRequestMessage(String appId, String appAttempt, int stageId) {
        this.appId = appId;
        this.appAttempt = appAttempt;
        this.stageId = stageId;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_FinishApplicationStageRequest;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, appId);
        ByteBufUtils.writeLengthAndString(buf, appAttempt);
        buf.writeInt(stageId);
    }

    public static FinishApplicationStageRequestMessage deserialize(ByteBuf buf) {
        String appId = ByteBufUtils.readLengthAndString(buf);
        String appAttempt = ByteBufUtils.readLengthAndString(buf);
        int stageId = buf.readInt();
        return new FinishApplicationStageRequestMessage(appId, appAttempt, stageId);
    }

    public String getAppId() {
        return appId;
    }

    public String getAppAttempt() {
        return appAttempt;
    }

    public int getStageId() {
        return stageId;
    }

    @Override
    public String toString() {
        return "FinishApplicationStageRequestMessage{" +
                "appId='" + appId + '\'' +
                ", appAttempt='" + appAttempt + '\'' +
                ", stageId=" + stageId +
                '}';
    }
}
