package org.bitnile.kalimandb.rpc.netty;

import io.netty.channel.ChannelHandlerContext;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.rpc.exception.RemotingConnectException;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.bitnile.kalimandb.rpc.protocol.SerializeType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RPCTest {
    private static final Logger log = LoggerFactory.getLogger(RPCTest.class);
    private static final String ADDR = "127.0.0.1:8899";
    private NettyRPCServer server;
    private NettyRPCClient client;

    @Before
    public void setUp() {
        server = new NettyRPCServer(new NettyServerConfig(8899));
        client = new NettyRPCClient(new NettyClientConfig());

        server.registerDefaultProcessor(new NettyRequestProcessor() {
            @Override
            public RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request) throws Exception {
                log.debug(request.toString());
                return RPCPacket.createResponsePacket(ResponseCode.SUCCESS, "success");
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newSingleThreadExecutor());

        server.start();
        client.start();
    }

    @Test
    public void invokeSync() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.TEST_REQUEST, null);
        request.setRequestId(3);
        request.setSerializeType(SerializeType.JSON);
        long startTime = System.currentTimeMillis();
        RPCPacket resp = client.invokeSync(ADDR, request, 2000L);
        log.info(String.valueOf(System.currentTimeMillis() - startTime));
        log.debug(resp.toString());
        assertNotNull(resp);
        assertEquals(resp.getCode(), ResponseCode.SUCCESS);
        assertEquals(resp.getRemark(), "success");
        assertEquals(resp.getRequestId(), 3);

    }

    @Test
    public void invokeOneway() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, RemotingTooMuchRequestException {
        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.TEST_REQUEST, null);
        client.invokeOneway(ADDR, request, 2000L);
        // wait for receiving
        TimeUnit.MILLISECONDS.sleep(2000L);
    }


    @Test
    public void invokeAsync() throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {
        CountDownLatch latch = new CountDownLatch(1);
        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.TEST_REQUEST, null);
        client.setCallbackExecutor(Executors.newSingleThreadExecutor());
        log.debug(request.toString());
        client.invokeAsync(ADDR, request, 3000L, (responseFuture) -> {
            RPCPacket resp = responseFuture.getResponse();
            log.info(resp.toString());
            assertNotNull(resp);
            assertEquals(resp.getCode(), ResponseCode.SUCCESS);
            assertEquals(resp.getRemark(), "success");
            latch.countDown();
        });
        // wait for receiving
        latch.await();

    }

    @After
    public void after() {
        client.shutdown();
        server.shutdown();
    }
}