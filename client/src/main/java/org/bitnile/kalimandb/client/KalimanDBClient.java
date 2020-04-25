package org.bitnile.kalimandb.client;

import org.bitnile.kalimandb.client.kaliman.KalimanDB;
import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.bitnile.kalimandb.rpc.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KalimanDBClient {
    private static final Logger logger = LoggerFactory.getLogger(KalimanDBClient.class);

    private final KalimanDBClientCore kalimanDBClientCore;

    public KalimanDBClient(NettyClientConfig nettyClientConfig, KalimanDBClientConfig kalimanDBClientConfig) {
        kalimanDBClientCore = new KalimanDBClientCore(nettyClientConfig, kalimanDBClientConfig);
    }

    public KalimanDB getDatabase(){
        return kalimanDBClientCore.getKalimanDB();
    }
}
