package org.bitnile.kalimandb.rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Encoder extends MessageToByteEncoder<RPCPacket> {
    private static final Logger logger = LoggerFactory.getLogger(Encoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RPCPacket msg, ByteBuf out){
        try {
            ByteBuffer header = null;
            header = msg.packetHeaderEncode();
            out.writeBytes(header);

            byte[] body = msg.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            logger.error("encode error, exception:", e);
            logger.error("msg info: {}", msg.toString());
            ctx.channel().close();
        }
    }
}
