package org.bitnile.kalimandb.rpc.exception;


public class RemotingSendRequestException extends RPCException {

    private static final long serialVersionUID = -6115113936079382253L;

    public RemotingSendRequestException(String addr, Throwable cause) {
        super("send request to <" + addr + "> failed", cause);
    }

}
