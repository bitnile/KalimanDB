package org.bitnile.kalimandb.service.option;

import org.bitnile.kalimandb.rpc.netty.NettyServerConfig;
import org.bitnile.kalimandb.raft.RaftConfig;
import org.bitnile.kalimandb.storage.option.StoreConfig;

public class KalimanDBServerConfig {
    private RaftConfig raftConfig;
    private ServiceConfig serviceConfig;
    private NettyServerConfig nettyServerConfig;
    private StoreConfig storeConfig;

    public KalimanDBServerConfig(RaftConfig raftConfig, ServiceConfig serviceConfig,
                                 NettyServerConfig nettyServerConfig, StoreConfig storeConfig) {
        this.raftConfig = raftConfig;
        this.serviceConfig = serviceConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.storeConfig = storeConfig;
    }

    public RaftConfig getRaftConfig() {
        return raftConfig;
    }

    public ServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public StoreConfig getStoreConfig() {
        return storeConfig;
    }
}
