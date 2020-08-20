package com.uber.rss.util;

import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class SystemUtils {

    public static long getFileDescriptorCount() {
        OperatingSystemMXBean mbean = ManagementFactory.getOperatingSystemMXBean();
        if(mbean instanceof UnixOperatingSystemMXBean){
            return ((UnixOperatingSystemMXBean)mbean).getOpenFileDescriptorCount();
        } else {
            return 0L;
        }
    }
}
