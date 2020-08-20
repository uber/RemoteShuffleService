package com.uber.rss.messages;

import com.uber.rss.common.MapTaskCommitStatus;
import com.uber.rss.util.ByteBufUtils;
import io.netty.buffer.ByteBuf;

/**
 * This is reponse for ConnectDownloadRequest.
 */
public class ConnectDownloadResponse extends ServerResponseMessage {
    private String serverId;
    private String serverVersion;
    private String runningVersion;
    private String compressionCodec;

    // this could be null
    private MapTaskCommitStatus mapTaskCommitStatus;

    // if dataAvailable is true, the server sends shuffle data immediately after this message
    private boolean dataAvailable;

    public ConnectDownloadResponse(String serverId, String serverVersion, String runningVersion, String compressionCodec, MapTaskCommitStatus mapTaskCommitStatus, boolean dataAvailable) {
        this.serverId = serverId;
        this.serverVersion = serverVersion;
        this.runningVersion = runningVersion;
        this.compressionCodec = compressionCodec;
        this.mapTaskCommitStatus = mapTaskCommitStatus;
        this.dataAvailable = dataAvailable;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_ConnectDownloadResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        ByteBufUtils.writeLengthAndString(buf, serverId);
        ByteBufUtils.writeLengthAndString(buf, serverVersion);
        ByteBufUtils.writeLengthAndString(buf, runningVersion);
        ByteBufUtils.writeLengthAndString(buf, compressionCodec);

        if (mapTaskCommitStatus == null) {
            buf.writeBoolean(false);
        } else {
            buf.writeBoolean(true);
            mapTaskCommitStatus.serialize(buf);
        }

        buf.writeBoolean(dataAvailable);
    }

    public static ConnectDownloadResponse deserialize(ByteBuf buf) {
        String serverId = ByteBufUtils.readLengthAndString(buf);
        String serverVersion = ByteBufUtils.readLengthAndString(buf);
        String runningVersion = ByteBufUtils.readLengthAndString(buf);
        String compressionCodec = ByteBufUtils.readLengthAndString(buf);

        MapTaskCommitStatus mapTaskCommitStatus = null;
        boolean mapTaskCommitStatusExisting = buf.readBoolean();
        if (mapTaskCommitStatusExisting) {
            mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
        }

        boolean dataAvailable = buf.readBoolean();

        return new ConnectDownloadResponse(serverId, serverVersion, runningVersion, compressionCodec, mapTaskCommitStatus, dataAvailable);
    }

    public String getServerId() {
        return serverId;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public String getRunningVersion() {
        return runningVersion;
    }

    public String getCompressionCodec() {
        return compressionCodec;
    }

    public MapTaskCommitStatus getMapTaskCommitStatus() {
        return mapTaskCommitStatus;
    }

    public boolean isDataAvailable() {
        return dataAvailable;
    }

    @Override
    public String toString() {
        return "ConnectDownloadResponse{" +
            "serverId='" + serverId + '\'' +
            ", serverVersion='" + serverVersion + '\'' +
            ", runningVersion='" + runningVersion + '\'' +
            ", compressionCodec='" + compressionCodec + '\'' +
            ", mapTaskCommitStatus=" + mapTaskCommitStatus +
            ", dataAvailable=" + dataAvailable +
            '}';
    }
}
