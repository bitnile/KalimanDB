package org.bitnile.kalimandb.common.protocol.header;

import org.bitnile.kalimandb.common.protocol.PacketCustomHeader;


public class DatabaseServiceHeader implements PacketCustomHeader {

    private byte method;

    private long leastSignificantBits;

    private long mostSignificantBits;

    public DatabaseServiceHeader(long leastSignificantBits, long mostSignificantBits) {
        this.leastSignificantBits = leastSignificantBits;
        this.mostSignificantBits = mostSignificantBits;
    }

    public DatabaseServiceHeader() {
    }

    public byte getMethod() {
        return method;
    }

    public void setMethod(byte method) {
        this.method = method;
    }

    public long getLeastSignificantBits() {
        return leastSignificantBits;
    }

    public void setLeastSignificantBits(long leastSignificantBits) {
        this.leastSignificantBits = leastSignificantBits;
    }

    public long getMostSignificantBits() {
        return mostSignificantBits;
    }

    public void setMostSignificantBits(long mostSignificantBits) {
        this.mostSignificantBits = mostSignificantBits;
    }


    @Override
    public String toString() {
        return "DatabaseServiceHeader{" +
                "method=" + method +
                ", leastSignificantBits=" + leastSignificantBits +
                ", mostSignificantBits=" + mostSignificantBits +
                '}';
    }
}
