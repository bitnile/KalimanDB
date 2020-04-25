package org.bitnile.kalimandb.common.exception;

public class ExecuteClosureException extends RuntimeException {
    public ExecuteClosureException() {
        super();
    }

    public ExecuteClosureException(String message) {
        super(message);
    }

    public ExecuteClosureException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExecuteClosureException(Throwable cause) {
        super(cause);
    }

    protected ExecuteClosureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
