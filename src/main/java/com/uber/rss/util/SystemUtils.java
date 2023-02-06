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

package com.uber.rss.util;

import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class SystemUtils {

    public static long getFileDescriptorCount() {
        OperatingSystemMXBean mbean = ManagementFactory.getOperatingSystemMXBean();
        if (mbean instanceof UnixOperatingSystemMXBean) {
            return ((UnixOperatingSystemMXBean)mbean).getOpenFileDescriptorCount();
        } else {
            return 0L;
        }
    }
}
