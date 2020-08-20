package com.uber.rss.clients;

import com.uber.rss.testutil.TestConstants;
import com.uber.rss.testutil.TestStreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class NotifyClientTest {
    private static final Logger logger = LoggerFactory.getLogger(NotifyClientTest.class);
    
    @Test
    public void finishApplicationJob() {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try (NotifyClient client = new NotifyClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
            client.connect();
            // send same request twice to make sure it is still good
            client.finishApplicationJob("app1", "exec1", 1, "success", null, null);
            client.finishApplicationJob("app1", "exec1", 1, "success", "", "");
            // send different request to make sure it is still good
            client.finishApplicationJob("app1", "exec1", 2, "fail", "RuntimeException", "Exception \nStacktrace");
        } finally {
            testServer.shutdown();
        }
    }

    @Test
    public void finishApplicationAttempt() {
        TestStreamServer testServer = TestStreamServer.createRunningServer();

        try (NotifyClient client = new NotifyClient("localhost", testServer.getShufflePort(), TestConstants.NETWORK_TIMEOUT, "user1")) {
            client.connect();
            // send same request twice to make sure it is still good
            client.finishApplicationAttempt("app1", "exec1");
            client.finishApplicationAttempt("app1", "exec1");
            // send different request to make sure it is still good
            client.finishApplicationAttempt("app1", "exec2");
            client.finishApplicationAttempt("app2", "exec2");
        } finally {
            testServer.shutdown();
        }
    }

}
