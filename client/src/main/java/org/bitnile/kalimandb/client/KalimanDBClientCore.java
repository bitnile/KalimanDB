package org.bitnile.kalimandb.client;

import org.bitnile.kalimandb.client.kaliman.KalimanDB;
import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.bitnile.kalimandb.client.factory.DefaultKalimanDBFactory;
import org.bitnile.kalimandb.client.factory.KalimanDBFactory;
import org.bitnile.kalimandb.common.DBVersion;
import org.bitnile.kalimandb.rpc.netty.NettyClientConfig;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;


public class KalimanDBClientCore {
    private final NettyClientConfig nettyClientConfig;
    private final KalimanDBClientConfig kalimanDBClientConfig;

    private KalimanDBFactory kalimanDBFactory;
    private DatabaseInstanceManager instanceManager;
    private KalimanDBClientRemotingService remotingService;

    private boolean isInit = false;

    public KalimanDBClientCore(NettyClientConfig nettyClientConfig, KalimanDBClientConfig kalimanDBClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.kalimanDBClientConfig = kalimanDBClientConfig;
    }

    public KalimanDB getKalimanDB() {
        if (!isInit) {
            synchronized (this) {
                if (!isInit) {
                    initDatabase();
                    isInit = true;
                }
            }
        }

        return kalimanDBFactory.getKalimanDB();
    }

    private void initDatabase() {
        checkConfig();
        kalimanDBClientConfig.checkConfig();
        nettyClientConfig.checkConfig();
        System.setProperty(RPCPacket.DB_VERSION, Integer.toString(DBVersion.CURRENT_VERSION));

        remotingService = new KalimanDBClientRemotingService(nettyClientConfig, kalimanDBClientConfig);
        instanceManager = new DatabaseInstanceManager(kalimanDBClientConfig);
        kalimanDBFactory = new DefaultKalimanDBFactory(instanceManager, remotingService);

        remotingService.start();
    }

    private void checkConfig() {
        if (nettyClientConfig == null) {
            throw new NullPointerException("NettyClientConfig");
        }

        if (kalimanDBClientConfig == null) {
            throw new NullPointerException("KalimanDBClientConfig");
        }
    }
}
