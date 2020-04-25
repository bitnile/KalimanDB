package org.bitnile.kalimandb.rpc.protocol;

import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class PacketMsgPackSerializerTest {
    private static final Logger log = LoggerFactory.getLogger(PacketMsgPackSerializerTest.class);
    private static final Serializer msgpack = SerializerFactory.get(SerializerType.MESSAGE_PACK);
    private static final Serializer json = SerializerFactory.get(SerializerType.JSON);

    @Test
    public void serializerTest() {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        header.setMethod((byte)0);
        RPCPacket packet = RPCPacket.createRequestPacket(RequestCode.TEST_REQUEST, header);
        PacketHeader packetHeader = PacketHeader.createPacketHeader(packet);

        // encode
        byte[] bs = PacketMsgPackSerializer.encode(packetHeader);

        // decodeHeader
        RPCPacket result = PacketMsgPackSerializer.decodeHeader(bs);

        // decodeCustomHeader
        DatabaseServiceHeader reusltHeader = (DatabaseServiceHeader) PacketMsgPackSerializer.decodeCustomHeader(result.getHeaderBytes(), DatabaseServiceHeader.class);

        // decodePacketHeader
        PacketHeader resultPacketHeader = PacketMsgPackSerializer.decodePacketHeader(bs);

        assertEquals(result.getCode(), RequestCode.TEST_REQUEST);
        assertEquals(reusltHeader.getMethod(), (byte)0);
    }



}