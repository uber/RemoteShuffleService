package com.uber.rss.messages;

import com.uber.rss.common.MapTaskCommitStatus;
import io.netty.buffer.ByteBuf;

/***
 * Response message for GetDataAvailabilityRequest
 */
public class GetDataAvailabilityResponse extends BaseMessage {

    // this could be null
    private MapTaskCommitStatus mapTaskCommitStatus;

    // if dataAvailable is true, the server sends shuffle data immediately after this message
    private boolean dataAvailable;

    public GetDataAvailabilityResponse(MapTaskCommitStatus mapTaskCommitStatus, boolean dataAvailable) {
        this.mapTaskCommitStatus = mapTaskCommitStatus;
        this.dataAvailable = dataAvailable;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_GetDataAvailabilityResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        if (mapTaskCommitStatus == null) {
            buf.writeBoolean(false);
        } else {
            buf.writeBoolean(true);
            mapTaskCommitStatus.serialize(buf);
        }

        buf.writeBoolean(dataAvailable);
    }

    public static GetDataAvailabilityResponse deserialize(ByteBuf buf) {
        MapTaskCommitStatus mapTaskCommitStatus = null;
        boolean mapTaskCommitStatusExisting = buf.readBoolean();
        if (mapTaskCommitStatusExisting) {
            mapTaskCommitStatus = MapTaskCommitStatus.deserialize(buf);
        }

        boolean dataAvailable = buf.readBoolean();

        return new GetDataAvailabilityResponse(mapTaskCommitStatus, dataAvailable);
    }

    public MapTaskCommitStatus getMapTaskCommitStatus() {
        return mapTaskCommitStatus;
    }

    public boolean isDataAvailable() {
        return dataAvailable;
    }

    @Override
    public String toString() {
        return "GetDataAvailabilityResponse{" +
            "mapTaskCommitStatus=" + mapTaskCommitStatus +
            "dataAvailable=" + dataAvailable +
            '}';
    }
}
