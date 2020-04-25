package org.bitnile.kalimandb.client;

import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.bitnile.kalimandb.common.exception.DatabaseClientException;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.common.protocol.ServiceEnum;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.rpc.exception.RemotingConnectException;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.netty.NettyClientConfig;
import org.bitnile.kalimandb.rpc.netty.NettyRPCClient;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KalimanDBClientRemotingService {
    private static final Logger logger = LoggerFactory.getLogger(KalimanDBClientRemotingService.class);
    private static final Serializer msgpackSerializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);
    private final NettyRPCClient nettyRPCClient;
    private final NettyClientConfig nettyClientConfig;
    private final KalimanDBClientConfig kalimanDBClientConfig;

    private static final int SERVICE_RETRY_TIMES = 3;

    public KalimanDBClientRemotingService(NettyClientConfig nettyClientConfig,
                                          KalimanDBClientConfig kalimanDBClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.kalimanDBClientConfig = kalimanDBClientConfig;
        this.nettyRPCClient = new NettyRPCClient(this.nettyClientConfig);
    }

    /**
     * In most cases, it is recommended that the caller use another construction method 
     * {@link KalimanDBClientRemotingService#KalimanDBClientRemotingService(NettyClientConfig, KalimanDBClientConfig)}
     */
    public KalimanDBClientRemotingService(NettyRPCClient nettyRPCClient,
                                          NettyClientConfig nettyClientConfig,
                                          KalimanDBClientConfig kalimanDBClientConfig) {
        this.nettyRPCClient = nettyRPCClient;
        this.nettyClientConfig = nettyClientConfig;
        this.kalimanDBClientConfig = kalimanDBClientConfig;
    }

    public RPCPacket sendRequest(String addr,
                                 DatabaseServiceHeader header,
                                 DatabaseServiceRequestArgs requestArgs,
                                 boolean isRetry) throws DatabaseClientException{
        int retry = 0;
        byte[] content = null;
        byte methodCode = header.getMethod();

        try {
            content = msgpackSerializer.write(requestArgs);
        } catch (Exception e) {
            throw new DatabaseClientException("Parameter encode error", e);
        }

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setBody(content);

        while (true) {
            try {
                RPCPacket response = null;
                response = nettyRPCClient.invokeSync(addr, request, kalimanDBClientConfig.getSendMsgTimeoutMillis());
                return response;
            } catch (RemotingTimeoutException | InterruptedException | RemotingSendRequestException e) {
                if (!isRetry) {
                    throw new DatabaseClientException("Send request error", e);
                }
                if (retry >= SERVICE_RETRY_TIMES) {
                    throw new DatabaseClientException("Retry to send the request " + retry + " times, but fail", e);
                }
                if (methodCode == ServiceEnum.FIND.getCode() || methodCode == ServiceEnum.FIND_STW.getCode())/* query request need not retry */ {
                    throw new DatabaseClientException("Find the document error", e);
                }
                retry++;
                logger.error("Send the request error", e);
            } catch (RemotingConnectException e) {
                throw new DatabaseClientException("Connect to leader " + addr + " error", e);
            }
        }


    }

    public void start() {
        nettyRPCClient.start();
    }
}
