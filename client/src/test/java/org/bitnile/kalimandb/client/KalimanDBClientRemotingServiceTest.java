package org.bitnile.kalimandb.client;

import javafx.beans.binding.When;
import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.bitnile.kalimandb.common.exception.DatabaseClientException;
import org.bitnile.kalimandb.common.protocol.ServiceEnum;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.rpc.exception.RemotingConnectException;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.netty.NettyClientConfig;
import org.bitnile.kalimandb.rpc.netty.NettyRPCClient;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KalimanDBClientRemotingServiceTest {
    private static final Logger log = LoggerFactory.getLogger(KalimanDBClientRemotingServiceTest.class);
    private static final String address = "127.0.0.1:8899";

    @Test
    public void sendRequest() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, DatabaseClientException {
        NettyRPCClient nettyRPCClient = mock(NettyRPCClient.class);

        KalimanDBClientRemotingService service =
                new KalimanDBClientRemotingService(nettyRPCClient, new NettyClientConfig(), new KalimanDBClientConfig());

        RPCPacket response = RPCPacket.createResponsePacket(0, "test");

        when(nettyRPCClient.invokeSync(anyString(), any(RPCPacket.class), anyLong())).thenReturn(response);

        DatabaseServiceHeader header = new DatabaseServiceHeader();
        header.setMethod(ServiceEnum.INSERT.getCode());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();

        RPCPacket packet = service.sendRequest(address, header, args,  true);
        assertEquals(packet, response);
    }

    @Test(expected = DatabaseClientException.class)
    public void sendRequestRemotingException() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, DatabaseClientException {
        NettyRPCClient nettyRPCClient = mock(NettyRPCClient.class);

        KalimanDBClientRemotingService service =
                new KalimanDBClientRemotingService(nettyRPCClient, new NettyClientConfig(), new KalimanDBClientConfig());

        RPCPacket response = RPCPacket.createResponsePacket(0, "test");

        when(nettyRPCClient.invokeSync(anyString(), any(RPCPacket.class), anyLong())).thenThrow(RemotingTimeoutException.class);

        DatabaseServiceHeader header = new DatabaseServiceHeader();
        header.setMethod(ServiceEnum.INSERT.getCode());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();

        service.sendRequest(address, header, args,  true);
    }

    @Test(expected = DatabaseClientException.class)
    public void sendRequestConnectionException() throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, DatabaseClientException {
        NettyRPCClient nettyRPCClient = mock(NettyRPCClient.class);

        KalimanDBClientRemotingService service =
                new KalimanDBClientRemotingService(nettyRPCClient, new NettyClientConfig(), new KalimanDBClientConfig());

        RPCPacket response = RPCPacket.createResponsePacket(0, "test");

        when(nettyRPCClient.invokeSync(anyString(), any(RPCPacket.class), anyLong())).thenThrow(RemotingConnectException.class);

        DatabaseServiceHeader header = new DatabaseServiceHeader();
        header.setMethod(ServiceEnum.INSERT.getCode());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();

        service.sendRequest(address, header, args,  true);

    }
}