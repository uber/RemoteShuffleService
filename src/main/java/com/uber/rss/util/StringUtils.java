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
