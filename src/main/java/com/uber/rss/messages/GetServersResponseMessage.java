package com.uber.rss.messages;

import com.uber.rss.common.ServerDetail;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class GetServersResponseMessage extends ServerResponseMessage {
    private List<ServerDetail> servers;

    public GetServersResponseMessage(List<ServerDetail> servers) {
        this.servers = servers;
    }

    @Override
    public int getMessageType() {
        return MessageConstants.MESSAGE_GetServersResponse;
    }

    @Override
    public void serialize(ByteBuf buf) {
        if (servers != null) {
            buf.writeInt(servers.size());
            for (ServerDetail s : servers) {
                s.serialize(buf);
            }
        } else {
            buf.writeInt(0);
        }
    }

    public static GetServersResponseMessage deserialize(ByteBuf buf) {
        int count = buf.readInt();
        List<ServerDetail> servers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ServerDetail serverDetail = ServerDetail.deserialize(buf);
            servers.add(serverDetail);
        }
        return new GetServersResponseMessage(servers);
    }

    public List<ServerDetail> getServers() {
        return servers;
    }

    @Override
    public String toString() {
        return "GetServersResponseMessage{" +
                "servers=" + servers +
                '}';
    }
}
