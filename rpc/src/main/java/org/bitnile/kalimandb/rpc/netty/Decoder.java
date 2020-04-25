package org.bitnile.kalimandb.rpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Decoder extends LengthFieldBasedFrameDecoder {
    private static final int FRAME_MAX_LENGTH = 16777216;  //3 Byte
    private static final Logger logger = LoggerFactory.getLogger(LengthFieldBasedFrameDecoder.class);

    public Decoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0 ,4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }

            ByteBuffer buffer = frame.nioBuffer();
            return RPCPacket.decode(buffer);
        } catch (Exception e) {
            logger.error("decode exception", e);
            ctx.close();
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
        return null;
    }
}
