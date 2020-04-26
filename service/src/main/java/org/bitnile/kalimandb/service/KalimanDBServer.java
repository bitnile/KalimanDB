package org.bitnile.kalimandb.service;

import org.bitnile.kalimandb.common.DBVersion;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.bitnile.kalimandb.service.option.KalimanDBServerConfig;
import org.bitnile.kalimandb.service.option.ServiceConfig;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.rpc.netty.NettyServerConfig;
import org.bitnile.kalimandb.raft.RaftConfig;
import org.bitnile.kalimandb.raft.RaftEngine;
import org.bitnile.kalimandb.storage.option.StoreConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * @author ITcathyh
 */
public class KalimanDBServer extends LifecycleBase {
    private KalimanDBService service;
    private KalimanDBRemote remote;

    static {
        System.setProperty(RPCPacket.DB_VERSION, Integer.toString(DBVersion.CURRENT_VERSION));
    }

    public KalimanDBServer(KalimanDBServerConfig config) throws IOException {
        Objects.requireNonNull(config);
        new KalimanDBServer(config.getNettyServerConfig(), config.getServiceConfig(),
                config.getRaftConfig(), config.getStoreConfig());
    }

    public KalimanDBServer(NettyServerConfig nettyServerConfig, ServiceConfig serviceConfig,
                           RaftConfig raftConfig, StoreConfig storeConfig) throws IOException {
        Objects.requireNonNull(nettyServerConfig);
        Objects.requireNonNull(serviceConfig);
        Objects.requireNonNull(raftConfig);
        Objects.requireNonNull(storeConfig);
        RaftEngine raftEngine = new RaftEngine(storeConfig, raftConfig);
        this.service = new KalimanDBServiceImpl(raftEngine, serviceConfig);
        this.remote = new KalimanDBRemote(this.service, nettyServerConfig);
    }

    @Override
    protected void initInternal() {
        service.init();
        remote.init();
    }

    @Override
    protected void startInternal() {
        service.start();
        remote.start();
    }

    @Override
    protected void stopInternal() {
        remote.stop();
        service.stop();
    }

    @Override
    protected void destroyInternal() {
        remote.destroy();
        service.destroy();
    }
}

