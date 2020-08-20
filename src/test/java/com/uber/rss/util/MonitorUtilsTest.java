package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorUtilsTest {
    
    @Test
    public void hasRssException() {
        Assert.assertFalse(MonitorUtils.hasRssException(null));
        Assert.assertFalse(MonitorUtils.hasRssException(""));
        Assert.assertFalse(MonitorUtils.hasRssException(" "));

        Assert.assertFalse(MonitorUtils.hasRssException("  abc \n def \r from com.uber \n\r"));

        Assert.assertFalse(MonitorUtils.hasRssException("  abc \n def \r from com.uber.rss.xyz \n\r"));

        Assert.assertFalse(MonitorUtils.hasRssException("  abc \n def \r abc.RuntimeException() \n\r"));
        Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.RssException() \n\r"));
        Assert.assertTrue(MonitorUtils.hasRssException("  abc \n def \r abc.RssXyzException() \n\r"));
    }

}
