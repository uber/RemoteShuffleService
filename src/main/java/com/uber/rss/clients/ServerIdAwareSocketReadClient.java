package com.uber.rss.clients;

import com.google.common.net.HostAndPort;
import com.uber.rss.common.AppShufflePartitionId;
import com.uber.rss.common.DownloadServerVerboseInfo;
import com.uber.rss.common.ServerDetail;
import com.uber.rss.exceptions.RssInvalidServerIdException;
import com.uber.rss.exceptions.RssInvalidServerVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/***
 * This client will check server id/version to make sure connecting to the correct server.
 */
public class ServerIdAwareSocketReadClient implements SingleServerReadClient {
    private static final Logger logger =
            LoggerFactory.getLogger(ServerIdAwareSocketReadClient.class);

    private final ServerDetail serverDetail;
    private SingleServerReadClient readClient;

    public ServerIdAwareSocketReadClient(ServerDetail serverDetail, int timeoutMillis, boolean compressed, int queueSize, String user, AppShufflePartitionId appShufflePartitionId, Collection<Long> latestTaskAttemptIds, long dataAvailablePollInterval, long dataAvailableWaitTime) {
        this.serverDetail = serverDetail;

        HostAndPort hostAndPort = HostAndPort.fromString(serverDetail.getConnectionString());
        String host = hostAndPort.getHostText();
        int port = hostAndPort.getPort();

        SingleServerReadClient client;
        if (compressed) {
            client = new CompressedRecordSocketReadClient(host, port, timeoutMillis, user, appShufflePartitionId, latestTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime);
        } else {
            client = new PlainRecordSocketReadClient(host, port, timeoutMillis, user, appShufflePartitionId, latestTaskAttemptIds, dataAvailablePollInterval, dataAvailableWaitTime);
        }
        if (queueSize > 0) {
            client = new BlockingQueueReadClient(client, queueSize, dataAvailableWaitTime);
        }
        this.readClient = client;
    }

    @Override
    public DownloadServerVerboseInfo connect() {
        DownloadServerVerboseInfo serverVerboseInfo;

        try {
            serverVerboseInfo = readClient.connect();
        } catch (Throwable ex) {
            close();
            throw ex;
        }

        if (!serverVerboseInfo.getId().equals(serverDetail.getServerId())) {
            close();
            String msg = String.format("Server id (%s) is not expected (%s)", serverVerboseInfo.getId(), serverDetail);
            throw new RssInvalidServerIdException(msg);
        }

        return serverVerboseInfo;
    }

    @Override
    public void close() {
        closeUnderlyingClient();
    }

    @Override
    public RecordKeyValuePair readRecord() {
        return readClient.readRecord();
    }

    @Override
    public long getShuffleReadBytes() {
        return readClient.getShuffleReadBytes();
    }

    @Override
    public String toString() {
        return "ServerIdAwareSocketReadClient{" +
            "serverDetail=" + serverDetail +
            ", readClient=" + readClient +
            '}';
    }

    private void closeUnderlyingClient() {
        if (readClient != null) {
            try {
                readClient.close();
            } catch (Throwable ex) {
                logger.warn(String.format("Failed to close underlying client %s", readClient), ex);
            }
            readClient = null;
        }
    }
}
