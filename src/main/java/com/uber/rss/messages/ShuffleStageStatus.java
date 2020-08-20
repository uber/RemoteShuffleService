package com.uber.rss.messages;

import com.uber.rss.common.MapTaskCommitStatus;
import com.uber.rss.exceptions.RssInvalidStateException;
import io.netty.buffer.ByteBuf;

/***
 * This class is used to tell shuffle read client the status of shuffle stage on the shuffle server.
 */
public class ShuffleStageStatus extends SerializableMessage {
    public static final byte FILE_STATUS_OK = 0;
    public static final byte FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED = 1;
    public static final byte FILE_STATUS_CORRUPTED = 2;

    private final byte fileStatus;
    private final MapTaskCommitStatus mapTaskCommitStatus;

    public ShuffleStageStatus(byte fileStatus, MapTaskCommitStatus mapTaskCommitStatus) {
        this.fileStatus = fileStatus;
        this.mapTaskCommitStatus = mapTaskCommitStatus;
    }

    @Override
    public void serialize(ByteBuf buf) {
        buf.writeByte(fileStatus);

        if (mapTaskCommitStatus == null) {
            buf.writeBoolean(false);
        } else {
            buf.writeBoolean(true);
            mapTaskCommitStatus.serialize(buf);
        }
    }

    public static ShuffleStageStatus deserialize(ByteBuf buf) {
        byte fileStatus = buf.readByte();

        MapTaskCommitStatus mapTaskCommitStatus = null;
        boolean mapTaskCommitStatusExisting = buf.readBoolean();
        if (mapTaskCommitStatusExisting) {
            mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
        }

        return new ShuffleStageStatus(fileStatus, mapTaskCommitStatus);
    }

    public byte getFileStatus() {
        return fileStatus;
    }

    public MapTaskCommitStatus getMapTaskCommitStatus() {
        return mapTaskCommitStatus;
    }

    public byte transformToMessageResponseStatus() {
        switch (fileStatus) {
            case ShuffleStageStatus.FILE_STATUS_OK:
                return MessageConstants.RESPONSE_STATUS_OK;
            case ShuffleStageStatus.FILE_STATUS_SHUFFLE_STAGE_NOT_STARTED:
                return MessageConstants.RESPONSE_STATUS_SHUFFLE_STAGE_NOT_STARTED;
            case ShuffleStageStatus.FILE_STATUS_CORRUPTED:
                return MessageConstants.RESPONSE_STATUS_FILE_CORRUPTED;
            default:
                throw new RssInvalidStateException(String.format("Invalid file status: %s", fileStatus));
        }
    }

    @Override
    public String toString() {
        return "ShuffleStageStatus{" +
            "fileStatus=" + fileStatus +
            ", mapTaskCommitStatus=" + mapTaskCommitStatus +
            '}';
    }
}
