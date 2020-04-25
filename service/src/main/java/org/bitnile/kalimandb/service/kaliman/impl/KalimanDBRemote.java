package org.bitnile.kalimandb.service.kaliman.impl;

import org.bitnile.kalimandb.common.DBVersion;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.bitnile.kalimandb.service.kaliman.KalimanDBService;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.rpc.netty.NettyRPCServer;
import org.bitnile.kalimandb.rpc.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KalimanDBRemote extends LifecycleBase {
    private static final Logger logger = LoggerFactory.getLogger(KalimanDBRemote.class);

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
        printLogo();

    }

    private void printLogo() {
        logger.info("\n" +
                "  _  __     _ _                       ____  ____  \n" +
                " | |/ /__ _| (_)_ __ ___   __ _ _ __ |  _ \\| __ ) \n" +
                " | ' // _` | | | '_ ` _ \\ / _` | '_ \\| | | |  _ \\ \n" +
                " | . \\ (_| | | | | | | | | (_| | | | | |_| | |_) |\n" +
                " |_|\\_\\__,_|_|_|_| |_| |_|\\__,_|_| |_|____/|____/ \n" +
                "                                                  \n" +
                "KalimanDB RELEASE 1.0");
    }
}
