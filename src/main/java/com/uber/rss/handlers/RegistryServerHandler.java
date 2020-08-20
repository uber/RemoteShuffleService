package com.uber.rss.handlers;

import com.uber.rss.common.ServerDetail;
import com.uber.rss.common.ServerDetailCollection;
import com.uber.rss.messages.GetServersRequestMessage;
import com.uber.rss.messages.GetServersResponseMessage;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.messages.RegisterServerRequestMessage;
import com.uber.rss.messages.RegisterServerResponseMessage;
import com.uber.rss.messages.ConnectRegistryRequest;
import com.uber.rss.messages.ConnectRegistryResponse;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/***
 * This class handles messages as a registry server, which stores available shuffle server information.
 */
public class RegistryServerHandler {
    private static final Logger logger = LoggerFactory.getLogger(RegistryServerHandler.class);

    private final ServerDetailCollection serverCollection;

    private final String serverId;

    public RegistryServerHandler(ServerDetailCollection serverCollection, String serverId) {
        this.serverCollection = serverCollection;
        this.serverId = serverId;
    }

    public void handleMessage(ChannelHandlerContext ctx, ConnectRegistryRequest msg) {
        logger.debug("Handle message: " + msg);

        ConnectRegistryResponse response = new ConnectRegistryResponse(serverId);
        HandlerUtil.writeResponseMsg(ctx, MessageConstants.RESPONSE_STATUS_OK, response, true);
    }

    public void handleMessage(ChannelHandlerContext ctx, RegisterServerRequestMessage request) {
        logger.info("Handling request: " + request);

        serverCollection.addServer(request.getDataCenter(), request.getCluster(), new ServerDetail(request.getServerId(), request.getRunningVersion(), request.getConnectionString()));

        RegisterServerResponseMessage response = new RegisterServerResponseMessage(request.getServerId());
        HandlerUtil.writeResponseMsg(ctx, response);
    }

    public void handleMessage(ChannelHandlerContext ctx, GetServersRequestMessage request) {
        logger.info("Handling request: " + request);

        List<ServerDetail> list = serverCollection.getServers(request.getDataCenter(), request.getCluster());

        int maxCount = Math.max(0, request.getMaxCount());
        if (list.size() > maxCount) {
            Collections.shuffle(list);
            list = list.subList(0, maxCount);
        }

        GetServersResponseMessage response = new GetServersResponseMessage(list);
        HandlerUtil.writeResponseMsg(ctx, response);
    }
}
