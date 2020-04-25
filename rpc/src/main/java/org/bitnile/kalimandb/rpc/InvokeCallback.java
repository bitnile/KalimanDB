package org.bitnile.kalimandb.rpc;


import org.bitnile.kalimandb.rpc.netty.ResponseFuture;

public interface InvokeCallback {
    void operationComplete(final ResponseFuture responseFuture);
}
