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

package com.uber.rss.metrics;

import java.util.Objects;

public class WriteClientMetricsKey {
    private String source;
    private String user;

    public WriteClientMetricsKey(String source, String user) {
        this.source = source;
        this.user = user;
    }

    public String getSource() {
        return source;
    }

    public String getUser() {
        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WriteClientMetricsKey that = (WriteClientMetricsKey) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, user);
    }

    @Override
    public String toString() {
        return "WriteClientMetricsKey{" +
                "source='" + source + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}
