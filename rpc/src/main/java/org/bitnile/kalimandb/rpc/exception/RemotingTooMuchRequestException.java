package org.bitnile.kalimandb.rpc.exception;


public class RemotingTooMuchRequestException extends RPCException {

    private static final long serialVersionUID = 1284383108659175269L;

    public RemotingTooMuchRequestException(String message) {
        super(message);
    }

}
