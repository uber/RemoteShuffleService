package com.uber.rss.handlers;

import com.uber.rss.common.ServerDetailCollection;
import com.uber.rss.exceptions.RssInvalidDataException;
import com.uber.rss.messages.GetServersRequestMessage;
import com.uber.rss.messages.RegisterServerRequestMessage;
import com.uber.rss.messages.ConnectRegistryRequest;
import com.uber.rss.metrics.M3Stats;
import com.uber.rss.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(RegistryChannelInboundHandler.class);

    private String connectionInfo = "";

    private final RegistryServerHandler serverHandler;

    public RegistryChannelInboundHandler(ServerDetailCollection serverCollection, String serverId) {
        serverHandler = new RegistryServerHandler(serverCollection, serverId);
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        processChannelActive(ctx);
    }

    public void processChannelActive(final ChannelHandlerContext ctx) {
        connectionInfo = NettyUtils.getServerConnectionInfo(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Got incoming message: " + msg + ", " + connectionInfo);
            }

            if (msg instanceof ConnectRegistryRequest) {
                serverHandler.handleMessage(ctx, (ConnectRegistryRequest)msg);
            } else if (msg instanceof RegisterServerRequestMessage) {
                serverHandler.handleMessage(ctx, (RegisterServerRequestMessage)msg);
            } else if (msg instanceof GetServersRequestMessage) {
                serverHandler.handleMessage(ctx, (GetServersRequestMessage)msg);
            } else {
                throw new RssInvalidDataException(String.format("Unsupported message: %s, %s", msg, connectionInfo));
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        M3Stats.addException(cause, this.getClass().getSimpleName());
        String msg = "Got exception " + connectionInfo;
        logger.warn(msg, cause);
        ctx.close();
    }

}
