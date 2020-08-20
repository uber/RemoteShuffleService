/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.rss;

import com.uber.rss.clients.RecordKeyValuePair;
import com.uber.rss.clients.SingleServerWriteClient;
import com.uber.rss.common.AppShuffleId;
import com.uber.rss.common.AppTaskAttemptId;
import com.uber.rss.exceptions.RssMissingShuffleWriteConfigException;
import com.uber.rss.exceptions.RssShuffleCorruptedException;
import com.uber.rss.testutil.ClientTestUtils;
import com.uber.rss.testutil.StreamServerTestUtils;
import com.uber.rss.testutil.TestStreamServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class StreamServerTest {
    private static final Logger logger = LoggerFactory.getLogger(StreamServerTest.class);
    
    @Test
    public void startAndShutdown() {
        StreamServer server1 = TestStreamServer.createRunningServer();
        StreamServer server2 = TestStreamServer.createRunningServer();
        
        server1.shutdown();
        server2.shutdown();
    }

    @Test
    public void readNonExistingShuffleData() {
        TestStreamServer testServer = TestStreamServer.createRunningServer();
        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (SingleServerWriteClient writeclient = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
            writeclient.connect();
            writeclient.startUpload(appTaskAttemptId, numMaps, 20);
            writeclient.sendRecord(1, null, null);
            writeclient.finishUpload();

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            boolean dataCompressed = false;
            int dataAvailableWaitTime = 500;

            AppShuffleId invalidAppShuffleId = new AppShuffleId("not_existing_app", appTaskAttemptId.getAppAttempt(), appTaskAttemptId.getShuffleId());
            try {
                StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), invalidAppShuffleId, 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataCompressed, dataAvailableWaitTime);
                Assert.fail("The previous code shall throw exception and shall not run into here");
            } catch (Throwable ex) {
                Assert.assertTrue(ex instanceof RssMissingShuffleWriteConfigException);
            }

            invalidAppShuffleId = new AppShuffleId(appTaskAttemptId.getAppId(), "not_existing_exec", appTaskAttemptId.getShuffleId());
            try {
                StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), invalidAppShuffleId, 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataCompressed, dataAvailableWaitTime);
                Assert.fail("The previous code shall throw exception and shall not run into here");
            } catch (Throwable ex) {
                Assert.assertTrue(ex instanceof RssMissingShuffleWriteConfigException);
            }

            invalidAppShuffleId = new AppShuffleId(appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt(), 912345);
            try {
                StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), invalidAppShuffleId, 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()), dataCompressed, dataAvailableWaitTime);
                Assert.fail("The previous code shall throw exception and shall not run into here");
            } catch (Throwable ex) {
                Assert.assertTrue(ex instanceof RssMissingShuffleWriteConfigException);
            }
        } finally {
            testServer.shutdown();
        }
    }
    
    @Test
    public void useLocalStandaloneRegistryServer() {
        TestStreamServer testServer = TestStreamServer.createRunningServerWithLocalStandaloneRegistryServer();

        int numMaps = 1;
        AppTaskAttemptId appTaskAttemptId = new AppTaskAttemptId("app1", "exec1", 1, 2, 0L);

        try (SingleServerWriteClient writeclient = ClientTestUtils.getOrCreateWriteClient(testServer.getShufflePort(), appTaskAttemptId.getAppId(), appTaskAttemptId.getAppAttempt())) {
            writeclient.connect();
            writeclient.startUpload(appTaskAttemptId, numMaps, 20);

            writeclient.sendRecord(1, null, null);

            writeclient.sendRecord(2,
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]));

            writeclient.sendRecord(3,
                    ByteBuffer.wrap("key1".getBytes(StandardCharsets.UTF_8)),
                    ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)));

            writeclient.finishUpload();

            // Verify read client able to read data from stream server.
            // We pass readQueueSize parameter to helper method readAllRecords, so it will use async read client.

            List<RecordKeyValuePair> records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 1, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            RecordKeyValuePair record = records.get(0);
            Assert.assertNull(record.getKey());
            Assert.assertNull(record.getValue());

            records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 2, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            record = records.get(0);
            Assert.assertEquals(record.getKey(), new byte[0]);
            Assert.assertEquals(record.getValue(), new byte[0]);

            records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 3, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 1);

            record = records.get(0);
            Assert.assertEquals(new String(record.getKey(), StandardCharsets.UTF_8), "key1");
            Assert.assertEquals(new String(record.getValue(), StandardCharsets.UTF_8), "value1");

            records = StreamServerTestUtils.readAllRecords(testServer.getShufflePort(), appTaskAttemptId.getAppShuffleId(), 4, Arrays.asList(appTaskAttemptId.getTaskAttemptId()));
            Assert.assertEquals(records.size(), 0);
        } finally {
            testServer.shutdown();
        }
    }

}
