package com.uber.rss.decoders;

import com.uber.rss.common.ServerDetailCollection;
import com.uber.rss.execution.ShuffleExecutor;
import com.uber.rss.handlers.NotifyChannelInboundHandler;
import com.uber.rss.handlers.RegistryChannelInboundHandler;
import com.uber.rss.handlers.UploadChannelManager;
import com.uber.rss.handlers.DownloadChannelInboundHandler;
import com.uber.rss.handlers.UploadChannelInboundHandler;
import com.uber.rss.messages.MessageConstants;
import com.uber.rss.util.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/***
 * The sole purpose of this class is to replace itself with the appropriate decoder(s) for the requested protocol version
 */
public class StreamServerVersionDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(StreamServerVersionDecoder.class);

    private final String serverId;
    private final String runningVersion;
    private final long idleTimeoutMillis;
    private final ShuffleExecutor executor;
    private final UploadChannelManager channelManager;
    private final String networkCompressionCodec;

    // this is used when the shuffle server could serve as a registry server
    private final ServerDetailCollection serverDetailCollection;

    public StreamServerVersionDecoder(String serverId,
                                      String runningVersion,
                                      long idleTimeoutMillis,
                                      ShuffleExecutor executor,
                                      UploadChannelManager channelManager,
                                      String networkCompressionCodec,
                                      ServerDetailCollection serverDetailCollection) {
        this.serverId = serverId;
        this.runningVersion = runningVersion;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.executor = executor;
        this.channelManager = channelManager;
        this.networkCompressionCodec = networkCompressionCodec;
        this.serverDetailCollection = serverDetailCollection;
    }

    private void addVersionDecoder(ChannelHandlerContext ctx, byte type, byte version) {
        ByteToMessageDecoder newDecoder;
        String decoderName = "decoder";
        ChannelInboundHandlerAdapter newHandler;
        String handlerName = "handler";

        if (type == MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE && version == MessageConstants.UPLOAD_UPLINK_VERSION_3) {
            newDecoder = new StreamServerMessageDecoder();
            UploadChannelInboundHandler channelInboundHandler = new UploadChannelInboundHandler(serverId, runningVersion, idleTimeoutMillis, executor, channelManager, networkCompressionCodec);
            channelInboundHandler.processChannelActive(ctx);
            newHandler = channelInboundHandler;
        } else if (type == MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE && version == MessageConstants.DOWNLOAD_UPLINK_VERSION_3) {
            newDecoder = new StreamServerMessageDecoder();
            DownloadChannelInboundHandler channelInboundHandler = new DownloadChannelInboundHandler(serverId, runningVersion, executor);
            channelInboundHandler.processChannelActive(ctx);
            newHandler = channelInboundHandler;
        } else if (type == MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE && version == MessageConstants.NOTIFY_UPLINK_VERSION_3) {
            newDecoder = new StreamServerMessageDecoder();
            NotifyChannelInboundHandler channelInboundHandler = new NotifyChannelInboundHandler(serverId);
            channelInboundHandler.processChannelActive(ctx);
            newHandler = channelInboundHandler;
        } else if (type == MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE && version == MessageConstants.REGISTRY_UPLINK_VERSION_3) {
            newDecoder = new StreamServerMessageDecoder();
            RegistryChannelInboundHandler channelInboundHandler = new RegistryChannelInboundHandler(serverDetailCollection, serverId);
            channelInboundHandler.processChannelActive(ctx);
            newHandler = channelInboundHandler;
        } else {
            String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
            logger.error(String.format(
                    "Invalid upload version %d for link type %s from client %s",
                    version, type, clientInfo));
            ctx.close();
            logger.info(String.format("Closed connection to client %s", clientInfo));
            return;
        }
        logger.debug(String.format("Using version %d protocol for client %s", version, NettyUtils.getServerConnectionInfo(ctx)));
        ctx.pipeline().replace(this, decoderName, newDecoder);
        ctx.pipeline().addAfter(decoderName, handlerName, newHandler);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx,
                          ByteBuf in,
                          List<Object> out) throws Exception {
        if (in.readableBytes() < 2 * Byte.BYTES) {
            return;
        }
        in.markReaderIndex();
        byte magicByte = in.readByte();
        byte version = in.readByte();
        in.resetReaderIndex();  // rewind so that the newly added decoder can re-read it

        switch (magicByte) {
            case MessageConstants.UPLOAD_UPLINK_MAGIC_BYTE:
            case MessageConstants.DOWNLOAD_UPLINK_MAGIC_BYTE:
            case MessageConstants.NOTIFY_UPLINK_MAGIC_BYTE:
            case MessageConstants.REGISTRY_UPLINK_MAGIC_BYTE:
                addVersionDecoder(ctx, magicByte, version);
                break;
            default:
                String clientInfo = NettyUtils.getServerConnectionInfo(ctx);
                logger.warn(String.format(
                        "Invalid magic byte %d from client %s",
                        magicByte, clientInfo));
                ctx.close();
                logger.info(String.format("Closed connection to client %s", clientInfo));
                break;
        }
    }

    // Newly added handlers will then re-process the message.
}