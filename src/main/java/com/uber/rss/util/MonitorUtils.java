package com.uber.rss.util;

import java.util.regex.Pattern;

public class MonitorUtils {

  private static final Pattern pattern = Pattern.compile("Rss\\w*Exception");

  public static boolean hasRssError(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }

    if (str.contains("com.uber.rss")) {
      return true;
    }

    return pattern.matcher(str).find();
  }
}
