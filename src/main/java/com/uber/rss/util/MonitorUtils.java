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

import java.util.regex.Pattern;

public class MonitorUtils {

  private static final Pattern pattern = Pattern.compile("Rss\\w*Exception");

  public static boolean hasRssException(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }

    if (str.contains("OutOfMemoryError")) {
      return true;
    }

    if (str.contains("KryoException")) {
      return true;
    }

    if (str.contains("exceeding memory limits")) {
      return true;
    }

    return pattern.matcher(str).find();
  }
}
