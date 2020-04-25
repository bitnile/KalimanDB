package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;

public class RaftConfig {

    private long cacheExpireTimeInMillis = 5;    // cache of command ID

    private String groupId = "KalimanDB";

    private String ip = "127.0.0.1";

    private int port = 8990;

    private PeerId serverId = JRaftUtils.getPeerId("127.0.0.1:8990");

    private RaftNodeOptions raftNodeOptions = new RaftNodeOptions();

    public long cacheExpireTimeInMillis() {
        return cacheExpireTimeInMillis;
    }

    public RaftConfig cacheExpireTimeInMillis(long cacheExpireTimeInMillis) {
        this.cacheExpireTimeInMillis = cacheExpireTimeInMillis;
        return this;
    }

    public NodeOptions getNodeOptions(){
        return raftNodeOptions.nodeOptions();
    }

    public RaftNodeOptions raftNodeOptions() {
        return raftNodeOptions;
    }

    public RaftConfig raftNodeOptions(RaftNodeOptions raftNodeOptions) {
        this.raftNodeOptions = raftNodeOptions;
        return this;
    }

    public String groupId() {
        return groupId;
    }

    public RaftConfig groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public PeerId serverId() {
        if(this.serverId == null){
            this.serverId = new PeerId(this.ip, this.port);
        }
        return serverId;
    }

    public RaftConfig serverId(PeerId serverId) {
        this.serverId = serverId;
        return this;
    }

    public RaftConfig serverId(String serverId){
        this.serverId = JRaftUtils.getPeerId(serverId);
        return this;
    }

    public String ip() {
        return ip;
    }

    public RaftConfig ip(String ip) {
        this.ip = ip;
        return this;
    }

    public long port() {
        return port;
    }

    public RaftConfig port(int port) {
        this.port = port;
        return this;
    }
}
