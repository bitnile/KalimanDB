package org.bitnile.kalimandb.service.status;

import org.bitnile.kalimandb.common.document.Document;

public class Result {
    //    private String id;
    private DBOperationStatus status;
    private String msg;
    private int errCode;
    private Document document;

    public Result(UpdateOperationResultBuilder builder) {
        this.status = builder.status;
        this.msg = builder.msg;
        this.errCode = builder.errCode;
        this.document = builder.document;
    }

    public static UpdateOperationResultBuilder builder() {
        return new UpdateOperationResultBuilder();
    }

    public DBOperationStatus getStatus() {
        return status;
    }

    public void setStatus(DBOperationStatus status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getErrCode() {
        return errCode;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public boolean isSuccess() {
        return status == DBOperationStatus.SUCCESS && errCode == 0;
    }

    public static class UpdateOperationResultBuilder {

        private DBOperationStatus status;
        private String msg;
        private int errCode;
        private Document document;

        public UpdateOperationResultBuilder status(DBOperationStatus status) {
            this.status = status;
            return this;
        }

        public UpdateOperationResultBuilder msg(String msg) {
            this.msg = msg;
            return this;
        }

        public UpdateOperationResultBuilder errCode(int errCode) {
            this.errCode = errCode;
            return this;
        }

        public UpdateOperationResultBuilder document(Document document) {
            this.document = document;
            return this;
        }

        public Result build() {
            return new Result(this);
        }
    }

}
