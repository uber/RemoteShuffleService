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

import org.apache.commons.lang3.math.NumberUtils;

public class StringUtils {
    private static final int KB_SIZE = 1024;
    private static final int MB_SIZE = 1024 * 1024;
    private static final int GB_SIZE = 1024 * 1024 * 1024;
    
    public static Long getBytesValue(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        String strLower = str.toLowerCase();
        int scale = 1;

         {
            if (strLower.endsWith("kb")) {
                strLower = strLower.substring(0, strLower.length() - 2).trim();
                scale = KB_SIZE;
            } if (strLower.endsWith("k")) {
                strLower = strLower.substring(0, strLower.length() - 1).trim();
                scale = KB_SIZE;
            } else if (strLower.endsWith("mb")) {
                strLower = strLower.substring(0, strLower.length() - 2).trim();
                scale = MB_SIZE;
            } else if (strLower.endsWith("m")) {
                strLower = strLower.substring(0, strLower.length() - 1).trim();
                scale = MB_SIZE;
            } else if (strLower.endsWith("gb")) {
                strLower = strLower.substring(0, strLower.length() - 2).trim();
                scale = GB_SIZE;
            } else if (strLower.endsWith("g")) {
                strLower = strLower.substring(0, strLower.length() - 1).trim();
                scale = GB_SIZE;
            } else if (strLower.endsWith("bytes")) {
                strLower = strLower.substring(0, strLower.length() - "bytes".length()).trim();
                scale = 1;
            }

            strLower = strLower.replace(",", "");

            if (!NumberUtils.isDigits(strLower)) {
                throw new RuntimeException("Invalid string for bytes: " + strLower);
            }

            double doubleValue = Double.parseDouble(strLower);
            return (long)(doubleValue * scale);
        }
    }
}
