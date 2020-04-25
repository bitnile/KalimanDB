package org.bitnile.kalimandb.rpc.exception;


public class RemotingConnectException extends Throwable {

    public RemotingConnectException(String addr) {
        this(addr, null);
    }


    public RemotingConnectException(String addr, Throwable cause) {
        super("connect to <" + addr + "> failed", cause);
    }
}
