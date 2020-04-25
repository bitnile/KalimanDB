package org.bitnile.kalimandb.rpc.protocol;

import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.*;

public class RPCPacketTest {
    private static final Serializer msgpack = SerializerFactory.get(SerializerType.MESSAGE_PACK);

    @Test
    public void createRequest() throws Exception {
        System.setProperty(RPCPacket.DB_VERSION, "2333");
        RPCPacket request = RPCPacket.createRequestPacket(1234, null);

        assertEquals(request.getVersion(), 2333);
        assertEquals(request.getCode(), 1234);
        assertEquals(request.getFlag() & 0x01, 0);

        RPCPacket response = RPCPacket.createResponsePacket(4321, null);
        assertEquals(response.getCode(), 4321);
        assertEquals(response.getFlag() & 0x01, 1);
    }

    @Test
    public void createResponse() throws Exception {
        System.setProperty(RPCPacket.DB_VERSION, "2333");
        RPCPacket response = RPCPacket.createResponsePacket(1234, "test response");

        assertEquals(response.getVersion(), 2333);
        assertEquals(response.getCode(), 1234);
        assertEquals(response.getFlag() & 0x01, 1);
        assertEquals(response.getRemark(), "test response");
    }



    @Test
    public void serialize() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader(12345,54321);
        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        RPCPacket request = RPCPacket.createRequestPacket(1234, header);
        request.setBody(msgpack.write(args));
        ByteBuffer buffer = request.packetHeaderEncode();
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() + request.getBody().length);
        newBuffer.put(buffer.array());
        newBuffer.put(request.getBody());

        // netty decoder will skip first 4 bytes
        byte[] bsNew = Arrays.copyOfRange(newBuffer.array(), 4, newBuffer.capacity());
        RPCPacket decode = RPCPacket.decode(ByteBuffer.wrap(bsNew));

        DatabaseServiceHeader decodeHeader = (DatabaseServiceHeader) decode.decodeCustomHeader(DatabaseServiceHeader.class);

        assertEquals(decode.getCode(), 1234);
        assertEquals(decodeHeader.getLeastSignificantBits(), header.getLeastSignificantBits());
        assertEquals(decodeHeader.getMostSignificantBits(), header.getMostSignificantBits());
    }

    @Test
    public void markProtocolTypeMessagePack() {
        int headerLength = 16777215;
        byte[] bs = RPCPacket.markProtocolType(headerLength, SerializeType.MESSAGEPACK);
        assertEquals(bs[0], 1);
        assertEquals(bs[1], -1);
        assertEquals(bs[2], -1);
        assertEquals(bs[3], -1);
        assertEquals(RPCPacket.getProtocolType(ByteBuffer.wrap(bs).getInt()), SerializeType.MESSAGEPACK);
    }

    @Test
    public void markProtocolTypeJSON() {
        int headerLength = 16777215;
        byte[] bs = RPCPacket.markProtocolType(headerLength, SerializeType.JSON);
        assertEquals(bs[0], 0);
        assertEquals(bs[1], -1);
        assertEquals(bs[2], -1);
        assertEquals(bs[3], -1);
        assertEquals(RPCPacket.getProtocolType(ByteBuffer.wrap(bs).getInt()), SerializeType.JSON);
    }

    @Test
    public void markOnewayRPC() {
        RPCPacket request = RPCPacket.createRequestPacket(1, null);
        request.markOnewayRPC();
        assertEquals(request.getFlag() & (1 << 1), 1 << 1);
        assertTrue(request.isOnewayRPC());
    }

    @Test
    public void markResponseType() {
        RPCPacket request = RPCPacket.createRequestPacket(1, null);
        assertFalse(request.isResponseType());

        RPCPacket respone = RPCPacket.createResponsePacket(1, null);
        assertTrue(respone.isResponseType());
    }
}