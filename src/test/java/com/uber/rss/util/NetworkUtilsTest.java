package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.ConnectException;

public class NetworkUtilsTest {
    private final int timeout = 100;
    
    @Test
    public void validHostName() {
        Assert.assertTrue(NetworkUtils.isReachable("localhost", timeout));
        Assert.assertTrue(NetworkUtils.isReachable(NetworkUtils.getLocalHostName(), timeout));
        Assert.assertTrue(NetworkUtils.isReachable(NetworkUtils.getLocalFQDN(), timeout));
    }

    @Test
    public void validIpAddress() {
        Assert.assertTrue(NetworkUtils.isReachable("127.0.0.1", timeout));
        Assert.assertTrue(NetworkUtils.isReachable("0000:0000:0000:0000:0000:0000:0000:0001", timeout));
        Assert.assertTrue(NetworkUtils.isReachable("::1", timeout));
    }

    @Test
    public void invalidHostName() {
        Assert.assertFalse(NetworkUtils.isReachable(null, timeout));
        Assert.assertFalse(NetworkUtils.isReachable("", timeout));
        Assert.assertFalse(NetworkUtils.isReachable(" ", timeout));
        Assert.assertFalse(NetworkUtils.isReachable("not_exist_host_abc_123", timeout));
    }
}
