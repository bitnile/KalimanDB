package org.bitnile.kalimandb.client.config;

import org.bitnile.kalimandb.common.utils.StringUtils;


public class KalimanDBClientConfig{
    private String groupId;
    private String raftAddr;
    private String serviceAddr;
    private int refreshLeaderTimeoutMillis = 2000;
    private long sendMsgTimeoutMillis = 3500L;


    public KalimanDBClientConfig(String groupId, String raftAddr, String serviceAddr, int refreshLeaderTimeoutMillis, long sendMsgTimeoutMillis) {
        this.groupId = groupId;
        this.raftAddr = raftAddr;
        this.serviceAddr = serviceAddr;
        this.refreshLeaderTimeoutMillis = refreshLeaderTimeoutMillis;
        this.sendMsgTimeoutMillis = sendMsgTimeoutMillis;
    }

    public KalimanDBClientConfig(String groupId, String raftAddr, String serviceAddr) {
        this(groupId, raftAddr, serviceAddr,  2000, 3500L);
    }

    public KalimanDBClientConfig() {
    }


    public void checkConfig() {
        if (!StringUtils.checkConfStr(raftAddr)) {
            throw new IllegalStateException("RaftAddr is invalid");
        }

        if (!StringUtils.checkConfStr(serviceAddr)) {
            throw new IllegalStateException("ServiceAddr is invalid");
        }
    }

    public String getServiceAddr() {
        return serviceAddr;
    }

    public void setServiceAddr(String serviceAddr) {
        this.serviceAddr = serviceAddr;
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

    public int getRefreshLeaderTimeoutMillis() {
        return refreshLeaderTimeoutMillis;
    }

    public void setRefreshLeaderTimeoutMillis(int refreshLeaderTimeoutMillis) {
        this.refreshLeaderTimeoutMillis = refreshLeaderTimeoutMillis;
    }

    public long getSendMsgTimeoutMillis() {
        return sendMsgTimeoutMillis;
    }

    public void setSendMsgTimeoutMillis(long sendMsgTimeoutMillis) {
        this.sendMsgTimeoutMillis = sendMsgTimeoutMillis;
    }
}
