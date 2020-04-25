package org.bitnile.kalimandb.rpc.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import org.bitnile.kalimandb.common.protocol.PacketCustomHeader;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * This is the Netty transmission protocol, based on Netty transmission, Data packets are divided into request and response
 * It provides factory methods, You can create a request packet with the {@link #createResponsePacket(int, String)},
 * or create a response packet with the {@link #createResponsePacket(int, String)}.
 *
 * Data packets are serialized / deserialized by MessagePack by default.
 * Of course, you can also choose to serialize / deserialize with JSON.
 *
 * Following is the composition of the packet:
 * <pre>
 *     There are 4 parts in the RPCPacket
 *
 * +---------------------------------------------------------------------------------------------+
 * |       Total Length (4 Bytes)    |     Serialize type + Packet Header Length (1B + 3B)       |
 * |---------------------------------|-----------------------------------------------------------|
 * |          Header Data            |                        Body Data                          |
 * +---------------------------------------------------------------------------------------------+
 * </pre>
 * <strong> Notes: After being decoded by Netty, the total length of the Packet is discarded. </strong>
 *
 *
 */
public class RPCPacket {
    public static final String DB_VERSION = "kalimandb.rpc.version";

    private static AtomicInteger requestIdGenerator = new AtomicInteger(0);
    private static SerializeType defaultSerializeType = SerializeType.MESSAGEPACK;
    private static final int RPC_TYPE_FLAG = 0; //0 REQUEST  1 RESPONSE
    private static final int RPC_ONEWAY_FLAG = 1;// 1 ONEWAY
    private static volatile int versionIndex = -1;

    /**
     * RequestCode or ResponseCode
     * {@link org.bitnile.kalimandb.common.protocol.RequestCode} {@link org.bitnile.kalimandb.common.protocol.ResponseCode}
     */
    private int code;

    /**
     * The language of the sender of the message
     */
    private LanguageCode languageCode = LanguageCode.JAVA;

    /**
     * Remoting Module Version
     */
    private int version;

    /**
     * RequestId used for match the response packet
     */
    private int requestId = requestIdGenerator.getAndIncrement();

    /**
     * Flag used for mark RPC_TYPE_FLAG and RPC_ONEWAY_FLAG
     */
    private int flag;

    /**
     * Some message from sender
     */
    private String remark = "";

    /**
     * PacketCustomHeader is used for implement specific functions
     */
    private transient PacketCustomHeader customHeader;

    /**
     * Used to save detailed information for specific functions
     */
    private transient byte[] body;

    /**
     * Bytes from serializing PacketCustomHeader by MessagePack
     */
    private transient byte[] headerBytes;
    private SerializeType serializeType = defaultSerializeType;

    public RPCPacket() {
    }

    public static RPCPacket createRequestPacket(int code, PacketCustomHeader customHeader) {
        RPCPacket packet = new RPCPacket();
        packet.setCode(code);
        packet.setCustomHeader(customHeader);
        markVersion(packet);
        return packet;
    }


    public static RPCPacket createResponsePacket(int code, String remark) {
        return createResponsePacket(code, remark, null);

    }

    public static RPCPacket createResponsePacket(int code, String remark, Class<? extends PacketCustomHeader> header) {
        RPCPacket packet = new RPCPacket();

        packet.setCode(code);
        markVersion(packet);
        packet.setRemark(remark);
        packet.markResponseType();

        if (header != null) {
            PacketCustomHeader packetCustomHeader = null;
            try {
                packetCustomHeader = header.newInstance();
                packet.setCustomHeader(packetCustomHeader);
            } catch (InstantiationException | IllegalAccessException e) {
                return null;
            }
        }

        return packet;
    }


    public ByteBuffer packetHeaderEncode() throws Exception {
        return this.packetHeaderEncode(this.body != null ? body.length : 0);
    }

    /**
     * Encode all information except body. Includes [Total Length, Serialize type, Packet Header Length, Header Data].
     *
     * @param bodyLength the length of message body
     * @return the encoded message header
     */
    public ByteBuffer packetHeaderEncode(final int bodyLength) throws Exception {
        // int :(SerializerType + Packet Header Length) 4 Bytes
        int length = 4;

        byte[] headerData = this.headerEncode();

        length += headerData.length;

        // allocate [Total Length + (SerializerType + Packet Header Length) + headerData.length] ByteBuffer
        ByteBuffer result = ByteBuffer.allocate(4 + length);

        length += bodyLength;

        // encode Total Length
        result.putInt(length);
        // encode SerializerType + Packet Header Length
        result.put(markProtocolType(headerData.length, serializeType));
        // encode Header Data
        result.put(headerData);
        result.flip();

        return result;

    }

    public static void markVersion(RPCPacket packet) {
        if (versionIndex >= 0) {
            packet.setVersion(versionIndex);
        } else {
            String ver = System.getProperty(DB_VERSION); // todo: setProperty (Creams)
            if (ver != null) {
                int verInt = Integer.parseInt(ver);
                packet.setVersion(verInt);
                versionIndex = verInt;
            }
        }
    }

    public static RPCPacket decode(ByteBuffer byteBuffer) {
        // After being decoded by Netty, the total length of the Packet is discarded
        int length = byteBuffer.limit();

        // SerializerType + Packet Header Length
        int serializeTypeAndHeaderLength = byteBuffer.getInt();

        // headerLength
        int headerLength = getHeaderLength(serializeTypeAndHeaderLength);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        RPCPacket packet = headerDecode(headerData, getProtocolType(serializeTypeAndHeaderLength));

        int bodylength = length - 4 - headerLength;
        byte[] body = null;
        if (bodylength > 0) {
            body = new byte[bodylength];
            byteBuffer.get(body);
        }
        packet.body = body;
        return packet;
    }



    public PacketCustomHeader decodeCustomHeader(Class<? extends PacketCustomHeader> headerClass){
        PacketCustomHeader result;
        result = PacketMsgPackSerializer.decodeCustomHeader(this.headerBytes, headerClass);
        this.setCustomHeader(result);
        return result;
    }

    private static RPCPacket headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case MESSAGEPACK:
                RPCPacket msgPackResult = PacketMsgPackSerializer.decodeHeader(headerData);
                msgPackResult.setSerializeType(type);
                return msgPackResult;
            case JSON:
                RPCPacket jsonResult = JSONSerializer.decode(headerData, RPCPacket.class);
                jsonResult.setSerializeType(type);
                return jsonResult;
            default:
                break;
        }
        return null;
    }


    private byte[] headerEncode() throws Exception {
        PacketHeader header = PacketHeader.createPacketHeader(this);
        if (SerializeType.MESSAGEPACK == serializeType) {
            return PacketMsgPackSerializer.encode(header);
        } else {
            return JSONSerializer.encode(header);
        }
    }

    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }

    @JSONField(serialize = false)
    public PacketType getType() {
        if (this.isResponseType()) {
            return PacketType.RESPONSE_PACKET;
        }

        return PacketType.REQUEST_PACKET;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE_FLAG;
        return (this.flag & bits) == bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY_FLAG;
        this.flag |= bits;
    }

    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY_FLAG;
        return (this.flag & bits) == bits;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE_FLAG;
        this.flag |= bits;
    }

    @Override
    public String toString() {
        return "RPCPacket{" +
                "code=" + code +
                ", languageCode=" + languageCode +
                ", version=" + version +
                ", requestId=" + requestId +
                ", flag=" + flag +
                ", remark='" + remark + '\'' +
                ", customHeader=" + customHeader +
                ", body=" + Arrays.toString(body) +
                ", headerBytes=" + Arrays.toString(headerBytes) +
                ", serializeType=" + serializeType +
                '}';
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public LanguageCode getLanguage() {
        return languageCode;
    }

    public void setLanguage(LanguageCode languageCode) {
        this.languageCode = languageCode;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public PacketCustomHeader getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(PacketCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public SerializeType getSerializeType() {
        return serializeType;
    }

    public void setSerializeType(SerializeType serializeType) {
        this.serializeType = serializeType;
    }

    public byte[] getHeaderBytes() {
        return headerBytes;
    }

    public void setHeaderBytes(byte[] headerBytes) {
        this.headerBytes = headerBytes;
    }
}
