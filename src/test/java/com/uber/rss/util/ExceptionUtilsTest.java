package com.uber.rss.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.ConnectException;

public class ExceptionUtilsTest {
    
    @Test
    public void getExceptionSimpleMessage() {
        Assert.assertEquals("", ExceptionUtils.getSimpleMessage(null));
        Assert.assertEquals("NullPointerException (null)", ExceptionUtils.getSimpleMessage(new NullPointerException()));
        Assert.assertEquals("NullPointerException ()", ExceptionUtils.getSimpleMessage(new NullPointerException("")));
        Assert.assertEquals("NullPointerException (null pointer)", ExceptionUtils.getSimpleMessage(new NullPointerException("null pointer")));
    }

    @Test
    public void isTimeoutException() {
        Assert.assertFalse(ExceptionUtils.isTimeoutException(null));
        Assert.assertFalse(ExceptionUtils.isTimeoutException(new RuntimeException((String)null)));
        Assert.assertFalse(ExceptionUtils.isTimeoutException(new RuntimeException("")));
        Assert.assertTrue(ExceptionUtils.isTimeoutException(new ConnectException("Connection Timed out in socket")));
        Assert.assertTrue(ExceptionUtils.isTimeoutException(new ConnectException("Connection timedout in socket")));
        Assert.assertTrue(ExceptionUtils.isTimeoutException(new ConnectException("Connection Time out in socket")));
        Assert.assertTrue(ExceptionUtils.isTimeoutException(new ConnectException("Connection timeout in socket")));
    }
}
