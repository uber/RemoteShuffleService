package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MonitorUtilsTest {
    
    @Test
    public void hasRssError() {
        Assert.assertFalse(MonitorUtils.hasRssError(null));
        Assert.assertFalse(MonitorUtils.hasRssError(""));
        Assert.assertFalse(MonitorUtils.hasRssError(" "));

        Assert.assertFalse(MonitorUtils.hasRssError("  abc \n def \r from com.uber \n\r"));

        Assert.assertTrue(MonitorUtils.hasRssError("  abc \n def \r from com.uber.rss.xyz \n\r"));

        Assert.assertFalse(MonitorUtils.hasRssError("  abc \n def \r abc.RuntimeException() \n\r"));
        Assert.assertTrue(MonitorUtils.hasRssError("  abc \n def \r abc.RssException() \n\r"));
        Assert.assertTrue(MonitorUtils.hasRssError("  abc \n def \r abc.RssXyzException() \n\r"));
    }

}
