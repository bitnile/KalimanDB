package org.bitnile.kalimandb.rpc.protocol;



public class PacketHeader {
    private int code;
    private byte languageCode;
    private int version;
    private int requestId;
    private int flag;
    private String remark;
    private transient byte[] headerBytes;

    public static PacketHeader createPacketHeader(RPCPacket packet) {
        PacketHeader packetHeader = new PacketHeader();
        packetHeader.setCode(packet.getCode());
        packetHeader.setLanguage(packet.getLanguage().getCode());
        packetHeader.setVersion(packet.getVersion());
        packetHeader.setRequestId(packet.getRequestId());
        packetHeader.setFlag(packet.getFlag());
        packetHeader.setRemark(packet.getRemark());

        if (packet.getCustomHeader() != null) {
            packetHeader.setHeaderBytes(PacketMsgPackSerializer.encode(packet.getCustomHeader()));
        }


        return packetHeader;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte getLanguage() {
        return languageCode;
    }

    public void setLanguage(byte languageCode) {
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

    public byte[] getHeaderBytes() {
        return headerBytes;
    }

    public void setHeaderBytes(byte[] headerBytes) {
        this.headerBytes = headerBytes;
    }
}
