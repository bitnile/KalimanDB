package org.bitnile.kalimandb.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import org.bitnile.kalimandb.rpc.InvokeCallback;
import org.bitnile.kalimandb.common.ExecutorServiceFactory;
import org.bitnile.kalimandb.rpc.common.Pair;
import org.bitnile.kalimandb.rpc.common.SemaphoreReleaseOnlyOnce;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.junit.Test;
import org.mockito.Spy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AbstractNettyRPCTest {
    @Spy
    AbstractNettyRPC abstractNettyRPC = new NettyRPCClient(new NettyClientConfig());

    @Test
    public void scanResponseTable() {
        int dummyId = 1;
        // mock timeout
        ResponseFuture responseFuture = new ResponseFuture(null,dummyId, -1000, new InvokeCallback() {
            @Override
            public void operationComplete(final ResponseFuture responseFuture) {
            }
        }, null);
        abstractNettyRPC.responseTable.putIfAbsent(dummyId, responseFuture);
        abstractNettyRPC.scanResponseTable();
        assertNull(abstractNettyRPC.responseTable.get(dummyId));
    }

    @Test
    public void processResponseCommandNullCallBack() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(0);
        ResponseFuture responseFuture = new ResponseFuture(null,1, 3000, null,
                new SemaphoreReleaseOnlyOnce(semaphore));

        abstractNettyRPC.responseTable.putIfAbsent(1, responseFuture);

        RPCPacket response = RPCPacket.createResponsePacket(0, "Foo");
        response.setRequestId(1);
        abstractNettyRPC.processResponseCommand(null, response);

        assertEquals(semaphore.availablePermits(), 1);
    }

    @Test
    public void processResponseCommand() throws InterruptedException {
        final Semaphore semaphore = new Semaphore(1);
        ResponseFuture responseFuture = new ResponseFuture(null,1, 3000, new InvokeCallback() {
            @Override
            public void operationComplete(final ResponseFuture responseFuture) {
                assertEquals(semaphore.availablePermits(), 0);
            }
        }, new SemaphoreReleaseOnlyOnce(semaphore));

        abstractNettyRPC.responseTable.putIfAbsent(1, responseFuture);

        RPCPacket response = RPCPacket.createResponsePacket(0, "Foo");
        response.setRequestId(1);
        abstractNettyRPC.processResponseCommand(null, response);

        // Acquire the release permit after call back
        semaphore.acquire(1);
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void processRequestPacket() throws Exception {
        NettyRequestProcessor processor = new NettyRequestProcessor() {
            @Override
            public RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request) throws Exception {
                assertEquals(request.getRequestId(), 0);
                return null;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        };

        ExecutorService executorService = ExecutorServiceFactory.newThreadPool();

        abstractNettyRPC.processorTable.put(1, new Pair<>(processor, executorService));
        RPCPacket request = RPCPacket.createRequestPacket(1, null);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        abstractNettyRPC.processRequestCommand(ctx, request);
    }

    @Test
    public void processRequestPacketReject() throws Exception {
        NettyRequestProcessor processor = new NettyRequestProcessor() {
            @Override
            public RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request) throws Exception {
                assertEquals(request.getRequestId(), 0);
                return null;
            }

            @Override
            public boolean rejectRequest() {
                return true;
            }
        };

        abstractNettyRPC.processorTable.put(1, new Pair<>(processor, Executors.newSingleThreadExecutor()));
        RPCPacket request = RPCPacket.createRequestPacket(1, null);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        abstractNettyRPC.processRequestCommand(ctx, request);
        verify(ctx).writeAndFlush(any());
    }

    @Test
    public void invokeImpl() throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        Channel channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        RPCPacket request = RPCPacket.createRequestPacket(1, null);

        when(channel.writeAndFlush(any())).thenReturn(future);
        try {
            abstractNettyRPC.invokeSyncImpl(channel, request, 3000);
        } catch (RemotingTimeoutException e) {
            // ignore
        }

        verify(channel).writeAndFlush(request);
    }
}
