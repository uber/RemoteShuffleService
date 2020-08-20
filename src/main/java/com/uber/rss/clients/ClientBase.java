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

package com.uber.rss.clients;

import com.uber.m3.tally.Stopwatch;
import com.uber.rss.exceptions.RssFileCorruptedException;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.exceptions.RssMissingShuffleWriteConfigException;
import com.uber.rss.exceptions.RssNetworkException;
import com.uber.rss.exceptions.RssShuffleStageNotStartedException;
import com.uber.rss.exceptions.RssServerBusyException;
import com.uber.rss.exceptions.RssStaleTaskAttemptException;
import com.uber.rss.exceptions.RssTooMuchDataException;
import com.uber.rss.messages.BaseMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.metrics.ClientConnectMetrics;
import com.uber.rss.metrics.ClientConnectMetricsKey;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.metrics.MetricGroupContainer;
import com.uber.rss.util.ByteBufUtils;
import com.uber.rss.util.ExceptionUtils;
import com.uber.rss.util.NetworkUtils;
import com.uber.rss.util.SocketUtils;
import com.uber.rss.util.ThreadUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/***
 * Base class for socket client, e.g. client connecting to control server.
 */
public abstract class ClientBase implements AutoCloseable {
    private static final Logger logger =
            LoggerFactory.getLogger(ClientBase.class);

    private static final AtomicLong internalClientIdSeed = new AtomicLong(0);

    private static final MetricGroupContainer<ClientConnectMetricsKey, ClientConnectMetrics>
        metricGroupContainer = new MetricGroupContainer<>(
            t->new ClientConnectMetrics(new ClientConnectMetricsKey(t.getSource(), t.getRemote())));

    protected final String host;
    protected final int port;
    protected final int timeoutMillis;

    protected Socket socket;
    protected InputStream inputStream;

    protected OutputStream outputStream;

    protected String connectionInfo = "";

    private final long internalClientId = internalClientIdSeed.getAndIncrement();

    public ClientBase(String host, int port, int timeoutMillis) {
        this.host = host;
        this.port = port;
        this.timeoutMillis = timeoutMillis;
        this.connectionInfo = String.format("%s %s [%s -> %s:%s]", this.getClass().getSimpleName(), internalClientId, NetworkUtils.getLocalHostName(), host, port);
        logger.debug(String.format("Created instance (timeout: %s millis): %s", timeoutMillis, this));
    }

    @Override
    public void close() {
        if (socket == null) {
            return;
        }

        try {
            if (outputStream != null) {
                outputStream.flush();
            }
        } catch (Throwable e) {
            logger.warn("Hit exception when flushing output stream: " + connectionInfo, e);
        }

        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (Throwable e) {
            logger.warn("Hit exception when closing output stream: " + connectionInfo, e);
        }

        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (Throwable e) {
            logger.warn("Hit exception when closing input stream: " + connectionInfo, e);
        }

        try {
            socket.close();
        } catch (Throwable e) {
            logger.warn("Hit exception when closing socket: " + connectionInfo, e);
        }

        try {
            metricGroupContainer.removeMetricGroup(getClientConnectMetricsKey());
        } catch (Throwable e) {
            logger.warn("Hit exception when removing metrics: " + connectionInfo, e);
        }

        socket = null;
    }

    @Override
    public String toString() {
        return connectionInfo;
    }

    protected boolean isClosed() {
        return socket == null;
    }

    protected void connectSocket() {
        long startTime = System.currentTimeMillis();
        int triedTimes = 0;
        try {
            // we see java.net.UnknownHostException sometimes due to DNS issue, thus retry on this exception
            Throwable lastException = null;
            while (System.currentTimeMillis() - startTime <= timeoutMillis) {
                ClientConnectMetrics metrics = metricGroupContainer.getMetricGroup(getClientConnectMetricsKey());
                if (triedTimes >= 1) {
                    logger.info(String.format("Retrying connect to %s:%s, total retrying times: %s, elapsed milliseconds: %s", host, port, triedTimes, System.currentTimeMillis() - startTime));
                    metrics.getSocketConnectRetries().update(triedTimes);
                }
                triedTimes++;
                Stopwatch clientConnectLatencyTimerStopwatch = metrics.getSocketConnectLatency().start();
                try {
                    socket = new Socket();
                    socket.setSoTimeout(timeoutMillis);
                    socket.setTcpNoDelay(true);
                    socket.connect(new InetSocketAddress(host, port), timeoutMillis);
                    break;
                } catch (UnknownHostException | NoRouteToHostException | ConnectException socketException) {
                    if (socketException instanceof ConnectException && !ExceptionUtils.isTimeoutException(socketException)) {
                        // not timeout exception, e.g. may be connection refused, no need to retry and throw out exception
                        throw socketException;
                    }
                    M3Stats.addException(socketException, this.getClass().getSimpleName());
                    socket = null;
                    lastException = socketException;
                    logger.info(String.format("Failed to connect to %s:%s, %s", host, port, ExceptionUtils.getSimpleMessage(socketException)));

                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (elapsedTime < timeoutMillis) {
                        ThreadUtils.sleep(Math.min(timeoutMillis - elapsedTime, ThreadUtils.SHORT_WAIT_TIME));
                    }
                } finally {
                    clientConnectLatencyTimerStopwatch.stop();
                }
            }

            if (socket == null) {
                if (lastException != null) {
                    throw lastException;
                } else {
                    throw new IOException(String.format("Failed to connect to %s:%s", host, port));
                }
            }

            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();

            connectionInfo = String.format("%s %s [%s -> %s (%s)]",
                    this.getClass().getSimpleName(),
                    internalClientId,
                    socket.getLocalSocketAddress(),
                    socket.getRemoteSocketAddress(),
                    host);
        } catch (Throwable e) {
            M3Stats.addException(e, this.getClass().getSimpleName());
            long elapsedTime = System.currentTimeMillis() - startTime;
            String msg = String.format("connectSocket failed after trying %s times for %s milliseconds (timeout set to %s): %s, %s", triedTimes, elapsedTime, timeoutMillis, connectionInfo, ExceptionUtils.getSimpleMessage(e));
            logger.warn(msg, e);
            throw new RssNetworkException(msg, e);
        }
    }
    
    protected void write(byte b) {
        try {
            outputStream.write(b);
        } catch (IOException e) {
            String logMsg = String.format("write failed: %s, %s", connectionInfo, ExceptionUtils.getSimpleMessage(e));
            logger.warn(logMsg, e);
            throw new RssNetworkException(logMsg, e);
        }
    }

    protected void writeMessageLengthAndContent(BaseMessage msg) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1000);
        byte[] bytes;
        try {
            msg.serialize(buf);
            bytes = ByteBufUtils.readBytes(buf);
        } finally {
            buf.release();
        }

        try {
            outputStream.write(ByteBufUtils.convertIntToBytes(bytes.length));
            outputStream.write(bytes);
            outputStream.flush();
        } catch (IOException e) {
            String logMsg = String.format("writeMessageLengthAndContent failed: %s, %s", connectionInfo, ExceptionUtils.getSimpleMessage(e));
            logger.warn(logMsg, e);
            throw new RssNetworkException(logMsg, e);
        }
    }

    protected void writeControlMessageNotWaitResponseStatus(BaseMessage msg) {
        logger.debug(String.format("Writing control message: %s, connection: %s", msg, connectionInfo));
        try {
            outputStream.write(ByteBufUtils.convertIntToBytes(msg.getMessageType()));
        } catch (IOException e) {
            String logMsg = String.format("write message type failed: %s, %s", connectionInfo, ExceptionUtils.getSimpleMessage(e));
            logger.warn(logMsg, e);
            throw new RssNetworkException(logMsg, e);
        }

        writeMessageLengthAndContent(msg);
    }

    protected void writeControlMessageAndWaitResponseStatus(BaseMessage msg) {
        writeControlMessageNotWaitResponseStatus(msg);

        readResponseStatus();
        logger.debug(String.format("Got OK response for control message: %s, connection: %s", msg, connectionInfo));
    }

    private int readStatus() {
        try {
            return inputStream.read();
        } catch (IOException e) {
            String logMsg = String.format("read status failed: %s, %s", connectionInfo, ExceptionUtils.getSimpleMessage(e));
            logger.warn(logMsg, e);
            throw new RssNetworkException(logMsg, e);
        }
    }

    protected void readHeaderResponseStatus() {
        int responseStatus = readStatus();
        checkHeaderResponseStatus(responseStatus);
    }

    protected void readResponseStatus() {
        int responseStatus = readStatus();
        checkOKResponseStatus(responseStatus);
    }
    
    private final void checkHeaderResponseStatus(int responseStatus) {
        if (responseStatus == MessageConstants.RESPONSE_STATUS_SERVER_BUSY) {
            throw new RssServerBusyException(String.format("Server busy: %s", connectionInfo));
        }

        checkOKResponseStatus(responseStatus);
    }

    private final void checkOKResponseStatus(int responseStatus) {
        switch (responseStatus) {
            case MessageConstants.RESPONSE_STATUS_OK:
                return;
            case MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED:
                throw new RssShuffleStageNotStartedException(String.format("Shuffle not started: %s", connectionInfo));
            case MessageConstants.RESPONSE_STATUS_SERVER_BUSY:
                throw new RssServerBusyException(String.format("Server busy: %s", connectionInfo));
            case MessageConstants.RESPONSE_STATUS_APP_TOO_MUCH_DATA:
                throw new RssTooMuchDataException(String.format("App writing too much data: %s", connectionInfo));
            case MessageConstants.RESPONSE_STATUS_FILE_CORRUPTED:
                throw new RssFileCorruptedException(String.format("Shuffle file corrupted or application writing too much data: %s", connectionInfo));
            case MessageConstants.RESPONSE_STATUS_MISSING_SHUFFLE_WRITE_CONFIG:
                throw new RssMissingShuffleWriteConfigException(String.format("Shuffle write config missing (status %s): %s", responseStatus, connectionInfo));
            case MessageConstants.RESPONSE_STATUS_STALE_TASK_ATTEMPT:
                throw new RssStaleTaskAttemptException(String.format("Task attempt is stale (there is a new task retry, thus the old task is not valid any more)", responseStatus, connectionInfo));
            default:
                throw new RssNetworkException(String.format("Response not ok: %s, %s", responseStatus, connectionInfo));
        }
    }

    private ClientConnectMetricsKey getClientConnectMetricsKey() {
        return new ClientConnectMetricsKey(this.getClass().getSimpleName(), host);
    }

    protected <R extends BaseMessage> R readResponseMessage(int messageId, Function<ByteBuf, R> deserializer) {
        int id = SocketUtils.readInt(inputStream);
        if (id != messageId) {
            throw new RssInvalidDataException(String.format("Expected message id: %s, actual message id: %s", messageId, id));
        }

        return readMessageLengthAndContent(deserializer);
    }

    protected <R extends BaseMessage> R readMessageLengthAndContent(Function<ByteBuf, R> deserializer) {
        int len = SocketUtils.readInt(inputStream);
        byte[] bytes = SocketUtils.readBytes(inputStream, len);

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
            R response = deserializer.apply(buf);
            return response;
        } finally {
            buf.release();
        }
    }
}
