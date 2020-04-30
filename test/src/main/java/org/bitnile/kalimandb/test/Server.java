package org.bitnile.kalimandb.test;

import com.alipay.sofa.jraft.conf.Configuration;
import org.apache.commons.io.FileUtils;
import org.bitnile.kalimandb.raft.RaftConfig;
import org.bitnile.kalimandb.raft.RaftNodeOptions;
import org.bitnile.kalimandb.rpc.netty.NettyServerConfig;
import org.bitnile.kalimandb.service.KalimanDBServer;
import org.bitnile.kalimandb.service.option.ServiceConfig;
import org.bitnile.kalimandb.storage.option.StoreConfig;
import java.io.File;
import java.io.IOException;

public class Server {

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.exit(1);
        }
        final String dbPathStr = args[0];
        final String groupIdStr = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];
        final String nettyPortStr = args[4];

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.port(Integer.parseInt(nettyPortStr));

        ServiceConfig serviceConfig = new ServiceConfig();

        Configuration initConf = new Configuration();
        initConf.parse(initConfStr);
        RaftConfig raftConfig = new RaftConfig();
        RaftNodeOptions raftNodeOptions = new RaftNodeOptions().electionTimeoutMs(1000).disableCli(false)
                .snapshotIntervalSecs(10).initialConf(initConf);
        raftConfig.raftNodeOptions(raftNodeOptions).groupId(groupIdStr).serverId(serverIdStr);

        StoreConfig storeConfig = new StoreConfig();
        storeConfig.dbPath(dbPathStr);
        FileUtils.forceMkdir(new File(dbPathStr, "raft"));

        KalimanDBServer kalimanDBServer = new KalimanDBServer(nettyServerConfig, serviceConfig, raftConfig, storeConfig);

        kalimanDBServer.init();
        kalimanDBServer.start();
    }
}
