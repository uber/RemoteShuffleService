package com.uber.rss;

import com.uber.rss.testutil.TestStreamServer;
import com.uber.rss.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StreamServerHttpTest {
    private static final Logger logger = LoggerFactory.getLogger(StreamServerHttpTest.class);

    @Test
    public void health()  {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try {
            String url = String.format("http://localhost:%s/health", testServer.getHttpPort());
            String response = HttpUtils.getUrl(url);
            Assert.assertEquals(response, "OK");
        } finally {
            testServer.shutdown();
        }
    }

    @Test
    public void threadDump()  {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try {
            String url = String.format("http://localhost:%s/threadDump", testServer.getHttpPort());
            String response = HttpUtils.getUrl(url);
            Assert.assertTrue(response.contains("ThreadStackTrace"));
            Assert.assertTrue(response.contains("main"));
        } finally {
            testServer.shutdown();
        }
    }

}
