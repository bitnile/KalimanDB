package org.bitnile.kalimandb.rpc.exception;


public class RemotingTimeoutException extends RPCException {

    private static final long serialVersionUID = -218408529558823744L;

    public RemotingTimeoutException(String message) {
        super(message);
    }

    public RemotingTimeoutException(String addr, long timeoutMillis, Throwable cause) {
        super("wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause);
    }

}
