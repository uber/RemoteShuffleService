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

package com.uber.rss.exceptions;

import com.uber.rss.metrics.M3Stats;

public class RssMapperDataMismatchException extends RuntimeException {

    private int numOfRecordsRead;
    private int numOfRecordsWritten;
    private String writeClient;

    public RssMapperDataMismatchException(int numOfRecordsRead, int numOfRecordsWritten, String writeClient) {
        this.numOfRecordsRead = numOfRecordsRead;
        this.numOfRecordsWritten = numOfRecordsWritten;
        this.writeClient = writeClient;
        M3Stats.addException(this, writeClient);
    }

    @Override
    public String getMessage() {
        return String.format("Number of records read and written did not match, %s records were read, " +
                "but %s records were written", this.numOfRecordsRead, this.numOfRecordsWritten);
    }

    @Override
    public String toString() {
        return "RssMapperDataMismatchException{" +
                " writeClient=" + this.writeClient +
                " numOfRecordsRead=" + this.numOfRecordsRead +
                ", numOfRecordsWritten=" + numOfRecordsWritten +
                "} " + super.toString();
    }
}
