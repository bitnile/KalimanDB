package org.bitnile.kalimandb.rpc;

import org.bitnile.kalimandb.rpc.exception.RemotingConnectException;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.netty.NettyRequestProcessor;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;

import java.util.concurrent.ExecutorService;

public interface RPCClient extends RPCService {

    /**
     * Send packets to the channel synchronously
     * @param addr Destination IP:PORT
     * @param request Request Packet
     * @param timeoutMillis expire date
     */
    RPCPacket invokeSync(final String addr, final RPCPacket request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException;

    /**
     * Send a oneway packets to the channel
     * @param addr Destination IP:PORT
     * @param request Request Packet
     * @param timeoutMillis expire date
     */
    void invokeOneway(final String addr, final RPCPacket request, final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, RemotingTooMuchRequestException;

    /**
     * @param requestCode which is in {@link org.bitnile.kalimandb.common.protocol.RequestCode}
     * @param processor the requestCode corresponding processor
     * @param executor the processor corresponding Executor
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);


    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * Send packets to the channel asynchronously
     * @param addr Destination IP:PORT
     * @param request Request Packet
     * @param timeoutMillis expire date
     */
    void invokeAsync(final String addr, final RPCPacket request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

}
