package org.bitnile.kalimandb.rpc.exception;

public class RPCException extends Exception {
    private static final long serialVersionUID = -2750724957744289553L;

    public RPCException() {
    }

    public RPCException(String message) {
        super(message);
    }

    public RPCException(String message, Throwable cause) {
        super(message, cause);
    }
}
