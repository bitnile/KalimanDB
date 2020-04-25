package org.bitnile.kalimandb.client;


import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import org.bitnile.kalimandb.client.config.KalimanDBClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

public class DatabaseInstanceManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInstanceManager.class);

    private KalimanDBClientConfig kalimanDBClientConfig;
    private ConcurrentMap<String/*IP address*/, String/*Service Port*/> databasePortTable = new ConcurrentHashMap<>();
    private String serviceAddr;
    private String groupId;
    private String raftAddr;

    /* used for test*/
    public DatabaseInstanceManager() {
    }

    public DatabaseInstanceManager(KalimanDBClientConfig kalimanDBClientConfig) {
        this.kalimanDBClientConfig = kalimanDBClientConfig;
        raftAddr = kalimanDBClientConfig.getRaftAddr();
        groupId = kalimanDBClientConfig.getGroupId();
        serviceAddr = kalimanDBClientConfig.getServiceAddr();

        initDatabasePortTable();
    }

    public PeerId selectLeader() throws TimeoutException, InterruptedException {
        Configuration conf = new Configuration();

        if (!conf.parse(raftAddr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + raftAddr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        BoltCliClientService cliClientService = new BoltCliClientService();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, kalimanDBClientConfig.getRefreshLeaderTimeoutMillis()).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        return RouteTable.getInstance().selectLeader(groupId);
    }

    private void initDatabasePortTable() {
        String[] serviceAddrs = kalimanDBClientConfig.getServiceAddr().split(",");
        String[] raftAddrs = kalimanDBClientConfig.getRaftAddr().split(",");

        int length = 0;
        if ((length = serviceAddrs.length) != raftAddrs.length) {
            throw new IllegalStateException("ServiceAddresses and RaftAddresses can not correspond");
        }

        for (int i = 0; i < length; i++) {
            String serviceIp = serviceAddrs[i].split(":")[0];
            String raftIp = raftAddrs[i].split(":")[0];

            if (!serviceIp.equals(raftIp)) {
                throw new IllegalStateException("ServiceAddresses and RaftAddresses can not correspond");
            }

            databasePortTable.put(raftAddrs[i], serviceAddrs[i]);
        }

        logger.info("Raft Addresses : {}", databasePortTable.keySet());
        logger.info("Service Addresses : {}", databasePortTable.values());
    }

    public String getServiceAddress(String address) {
        return databasePortTable.get(address);
    }


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getRaftAddr() {
        return raftAddr;
    }

    public void setRaftAddr(String raftAddr) {
        this.raftAddr = raftAddr;
    }

    public String getServiceAddr() {
        return serviceAddr;
    }

    public void setServiceAddr(String serviceAddr) {
        this.serviceAddr = serviceAddr;
    }
}
