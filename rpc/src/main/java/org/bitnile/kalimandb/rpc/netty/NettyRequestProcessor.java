package org.bitnile.kalimandb.rpc.netty;

import io.netty.channel.ChannelHandlerContext;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;

/**
 * Common remoting command processor
 */
public interface NettyRequestProcessor {
    RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request)
        throws Exception;

    boolean rejectRequest();
}
