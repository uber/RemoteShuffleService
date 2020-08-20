package com.uber.rss.util;

public class LogUtils {

    public static double calculateMegaBytesPerSecond(long durationMillis, long bytes) {
        if (durationMillis == 0) {
            return 0;
        }
        return ((double)bytes)/(1024.0*1024.0)/(((double)durationMillis)/1000.0);
    }

}
