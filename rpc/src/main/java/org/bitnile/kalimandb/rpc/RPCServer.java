package org.bitnile.kalimandb.rpc;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.bitnile.kalimandb.rpc.common.Pair;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.netty.NettyRequestProcessor;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;

import java.util.concurrent.ExecutorService;

public interface RPCServer extends RPCService {

    /**
     * @return The port the server is listening on
     */
    int localListenPort();

    /**
     * Register the requestCode, processor and executor to the server.When the server receives the {@link RPCPacket},
     * it can find the registered processor corresponding to the code,and execute the
     * {@link NettyRequestProcessor#processRequest(ChannelHandlerContext, RPCPacket)} through the executor.
     *
     * @param requestCode which is in {@link org.bitnile.kalimandb.common.protocol.RequestCode}
     * @param processor the requestCode corresponding processor
     * @param executor the processor corresponding Executor
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * DefaultProcessor: The processor to execute when the corresponding request number cannot be found
     * @param processor processor
     * @param executor the processor corresponding Executor
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * @param requestCode Request code corresponding to the processor
     * @return Processor and executor combination
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);


    /**
     * Send packets to the channel synchronously
     * @param channel Destination channel
     * @param request Request Packet
     * @param timeoutMillis expire date
     */
    RPCPacket invokeSync(final Channel channel, final RPCPacket request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * Send oneway packets to the channel
     * @param channel Destination channel
     * @param request Request Packet
     * @param timeoutMillis expire date
     */
    void invokeOneway(final Channel channel, final RPCPacket request, final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingTooMuchRequestException;

    //    void invokeAsync(final Channel channel, final RPCPacket request, final long timeoutMillis,
    //                     final InvokeCallback invokeCallback);



}
