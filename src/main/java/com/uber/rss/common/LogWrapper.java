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

package com.uber.rss.common;

import com.uber.rss.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogWrapper {
    private static final Logger logger = LoggerFactory.getLogger(LogWrapper.class);

    /**
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * @param jsonLogger
     * @param map
     */
    public static final void logAsJson(Logger jsonLogger, Map<String, Object> map) {
        try {
            map.put("ts", System.currentTimeMillis());
            String json = JsonUtils.serialize(map);
            jsonLogger.info("JSON_LOG: " + json);
        } catch (Throwable ex) {
            logger.warn("Failed to log json", ex);
        }
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k
     * @param v
     */
    public static final void logAsJson(Logger jsonLogger, String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put(k, v);
        logAsJson(jsonLogger, map);
    }

    /**
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        logAsJson(jsonLogger, map);
    }

    /**
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        logAsJson(jsonLogger, map);
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     * @param k4
     * @param v4
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        logAsJson(jsonLogger, map);
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     * @param k4
     * @param v4
     * @param k5
     * @param v5
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        map.put(k5, v5);
        logAsJson(jsonLogger, map);
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     * @param k4
     * @param v4
     * @param k5
     * @param v5
     * @param k6
     * @param v6
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5, String k6, Object v6) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        map.put(k5, v5);
        map.put(k6, v6);
        logAsJson(jsonLogger, map);
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     * @param k4
     * @param v4
     * @param k5
     * @param v5
     * @param k6
     * @param v6
     * @param k7
     * @param v7
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5, String k6, Object v6, String k7, Object v7) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        map.put(k5, v5);
        map.put(k6, v6);
        map.put(k7, v7);
        logAsJson(jsonLogger, map);
    }

    /***
     * Log data as json. This is to help log processing tooling to parse the log and extract data.
     * There are multiple method overloads with different number of key/value pairs. That comes from
     * the idea in Java 9 Map.of() method, which has similar multiple overloads
     * (https://docs.oracle.com/javase/9/docs/api/java/util/Map.html#of--).
     * @param jsonLogger
     * @param k1
     * @param v1
     * @param k2
     * @param v2
     * @param k3
     * @param v3
     * @param k4
     * @param v4
     * @param k5
     * @param v5
     * @param k6
     * @param v6
     * @param k7
     * @param v7
     * @param k8
     * @param v8
     */
    public static final void logAsJson(Logger jsonLogger, String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4, String k5, Object v5, String k6, Object v6, String k7, Object v7, String k8, Object v8) {
        Map<String, Object> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        map.put(k5, v5);
        map.put(k6, v6);
        map.put(k7, v7);
        map.put(k8, v8);
        logAsJson(jsonLogger, map);
    }
}
