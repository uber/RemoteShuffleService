package com.uber.rss.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class NettyUtils {
    public static String getRemoteAddressAsString(ChannelHandlerContext ctx) {
        return ctx.channel().remoteAddress().toString();
    }

    public static String getServerConnectionInfo(ChannelHandlerContext ctx) {
        return getServerConnectionInfo(ctx.channel());
    }

    public static String getServerConnectionInfo(Channel channel) {
        return String.format("[%s -> %s]", channel.localAddress(), channel.remoteAddress());
    }
}
