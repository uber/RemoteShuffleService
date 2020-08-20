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

package com.uber.rss;

import com.uber.rss.exceptions.RssException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RssBuildInfo {
    public static String UnknownValue = "<unknown>";

    public static String Version;
    public static String Revision;

    static {
        String fileName = "rss-build-info.properties";
        try (InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
            if (resourceStream == null) {
                throw new RssException(String.format("Could not find info file: %s", fileName));
            }

            Properties props = new Properties();
            props.load(resourceStream);

            Version = props.getProperty("version", UnknownValue);
            Revision = props.getProperty("revision", UnknownValue);
        } catch (IOException e) {
            throw new RssException(String.format("Failed to open info file: %s", fileName));
        }
    }
}
