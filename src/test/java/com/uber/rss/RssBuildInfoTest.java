package com.uber.rss;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RssBuildInfoTest {
    @Test
    public void versionInfo() {
        Assert.assertFalse(StringUtils.isBlank(RssBuildInfo.Version));
        Assert.assertNotEquals(RssBuildInfo.Version, RssBuildInfo.UnknownValue);

        Assert.assertFalse(StringUtils.isBlank(RssBuildInfo.Revision));
        Assert.assertNotEquals(RssBuildInfo.Revision, RssBuildInfo.UnknownValue);
    }

}
