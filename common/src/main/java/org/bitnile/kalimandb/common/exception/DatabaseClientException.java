package org.bitnile.kalimandb.common.exception;


public class DatabaseClientException extends Exception {

    private static final long serialVersionUID = 2998345309504214240L;

    public DatabaseClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseClientException(String message) {
        super(message);
    }

    public DatabaseClientException(String groupId, String confStr) {
        this("GroupId: " + groupId + ", " + "confStr:" + confStr + "not found");
    }

    public DatabaseClientException(Throwable cause) {
        super(cause);
    }

    public DatabaseClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DatabaseClientException() {
    }
}
