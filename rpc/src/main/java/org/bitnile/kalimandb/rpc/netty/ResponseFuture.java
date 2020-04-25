package org.bitnile.kalimandb.rpc.netty;

import io.netty.channel.Channel;
import org.bitnile.kalimandb.rpc.InvokeCallback;
import org.bitnile.kalimandb.rpc.common.SemaphoreReleaseOnlyOnce;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    private final int requestId;
    private final Channel processChannel;
    private final long timeoutMillis;
    private final InvokeCallback invokeCallback;
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final SemaphoreReleaseOnlyOnce once;

    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile RPCPacket response;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(Channel channel, int requestId, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this.requestId = requestId;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public RPCPacket waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.response;
    }

    public void putResponse(final RPCPacket rpcPacket) {
        this.response = rpcPacket;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RPCPacket getResponse() {
        return response;
    }

    public void setRPCPacket(RPCPacket rpcPacket) {
        this.response = rpcPacket;
    }

    public int getrequestId() {
        return requestId;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    @Override
    public String toString() {
        return "ResponseFuture [RPCPacket=" + response
            + ", sendRequestOK=" + sendRequestOK
            + ", cause=" + cause
            + ", requestId=" + requestId
            + ", processChannel=" + processChannel
            + ", timeoutMillis=" + timeoutMillis
            + ", invokeCallback=" + invokeCallback
            + ", beginTimestamp=" + beginTimestamp
            + ", countDownLatch=" + countDownLatch + "]";
    }
}
