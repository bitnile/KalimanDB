package org.bitnile.kalimandb.common.protocol.body;


import org.bitnile.kalimandb.common.document.Document;

import java.util.Arrays;

public class DatabaseServiceResult {
    private int status;
    private String msg;
    private int code;
    private byte[][] documentsBytes;

    public DatabaseServiceResult() {
    }

    public DatabaseServiceResult(int status, String msg, int code, byte[][] documentsBytes) {
        this.status = status;
        this.msg = msg;
        this.code = code;
        this.documentsBytes = documentsBytes;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public byte[][] getDocumentsBytes() {
        return documentsBytes;
    }

    public void setDocumentsBytes(byte[][] documentsBytes) {
        this.documentsBytes = documentsBytes;
    }

    @Override
    public String toString() {
        return "DatabaseServiceResult{" +
                "status=" + status +
                ", msg='" + msg + '\'' +
                ", code=" + code +
                ", documentsBytes=" + Arrays.toString(documentsBytes) +
                '}';
    }
}
