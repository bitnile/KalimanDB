package org.bitnile.kalimandb.service.kaliman.impl;

import org.bitnile.kalimandb.common.DBVersion;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.bitnile.kalimandb.service.kaliman.KalimanDBService;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.rpc.netty.NettyRPCServer;
import org.bitnile.kalimandb.rpc.netty.NettyServerConfig;


public class KalimanDBRemote extends LifecycleBase {
    private final NettyRPCServer nettyRPCServer;
    private final NettyServerConfig nettyServerConfig;
    private ServerRemotingProcessor serverRemotingProcessor;

    private KalimanDBService kalimanDBService;

    public KalimanDBRemote(KalimanDBService kalimanDBService, NettyServerConfig nettyServerConfig) {
        this.kalimanDBService = kalimanDBService;

        this.nettyServerConfig = nettyServerConfig;
        this.nettyRPCServer = new NettyRPCServer(this.nettyServerConfig);
        this.serverRemotingProcessor = new ServerRemotingProcessor(this.kalimanDBService);

        registerProcessor();
    }

    private void registerProcessor() {
        this.nettyRPCServer.registerProcessor(RequestCode.DATABASE_SERVICE, serverRemotingProcessor, null);
    }

    @Override
    protected void startInternal() {
        nettyRPCServer.start();
    }
}
