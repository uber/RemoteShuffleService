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

package com.uber.rss.clients;

/***
 * This class is a marker to put into the record queue in async read client, so it knows 
 * there is an underlying failure.
 */
public class FailedFetchRecordKeyValuePair extends RecordKeyValuePair {
    private Throwable cause;
    
    public FailedFetchRecordKeyValuePair(Throwable cause) {
        super(null, null, 0L);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }
}
