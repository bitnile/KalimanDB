package org.bitnile.kalimandb.rpc.protocol;

import org.bitnile.kalimandb.common.protocol.PacketCustomHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacketMsgPackSerializer {
    private static final Serializer serializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);
    private static final Logger logger = LoggerFactory.getLogger(PacketMsgPackSerializer.class);

    public static byte[] encode(Object obj) {
        return serializer.write(obj);
    }

    public static PacketHeader decodePacketHeader(byte[] bytes){
        return serializer.read(bytes, PacketHeader.class);
    }

    public static PacketCustomHeader decodeCustomHeader(byte[] bytes, Class<? extends PacketCustomHeader> packetCustomHeader) {
        return serializer.read(bytes, packetCustomHeader);
    }

    public static RPCPacket decodeHeader(byte[] headerData) {
        RPCPacket packet = new RPCPacket();

        PacketHeader header = null;

        header = decodePacketHeader(headerData);
        packet.setCode(header.getCode());
        packet.setLanguage(LanguageCode.valueOf(header.getLanguage()));
        packet.setVersion(header.getVersion());
        packet.setFlag(header.getFlag());
        packet.setRequestId(header.getRequestId());
        packet.setRemark(header.getRemark());
        packet.setHeaderBytes(header.getHeaderBytes());
        return packet;
    }
}
