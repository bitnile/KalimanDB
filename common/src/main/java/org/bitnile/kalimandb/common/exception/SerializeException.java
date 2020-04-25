package org.bitnile.kalimandb.common.exception;

/**
 * @author Creams
 */
public class SerializeException extends RuntimeException {
    private static final long serialVersionUID = 17689341239358903L;

    public SerializeException() {
        super();
    }

    public SerializeException(String message) {
        super(message);
    }

    public SerializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializeException(Throwable cause) {
        super(cause);
    }

    protected SerializeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
