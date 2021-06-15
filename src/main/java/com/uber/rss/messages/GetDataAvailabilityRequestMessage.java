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

import io.netty.buffer.ByteBuf;

/***
 * Message sent by the client to check shuffle data availability
 */
public class GetDataAvailabilityRequestMessage extends BaseMessage {

    public GetDataAvailabilityRequestMessage() {
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_GetDataAvailabilityRequestMessage;
    }

    @Override
    public void serialize(ByteBuf buf) {
    }

    public static GetDataAvailabilityRequest deserialize(ByteBuf buf) {
        return new GetDataAvailabilityRequest();
    }

    @Override
    public String toString() {
        return "GetDataAvailabilityRequest{" +
                '}';
    }
}