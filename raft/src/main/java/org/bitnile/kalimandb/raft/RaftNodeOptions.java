package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;

public class RaftNodeOptions {

    private NodeOptions nodeOptions = new NodeOptions();

    public JRaftServiceFactory serviceFactory() {
        return nodeOptions.getServiceFactory();
    }

    public RaftNodeOptions serviceFactory(final JRaftServiceFactory serviceFactory) {
        nodeOptions.setServiceFactory(serviceFactory);
        return this;
    }

    public SnapshotThrottle snapshotThrottle() {
        return nodeOptions.getSnapshotThrottle();
    }

    public RaftNodeOptions snapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        nodeOptions.setSnapshotThrottle(snapshotThrottle);
        return this;
    }

    public boolean enableMetrics() {
        return nodeOptions.isEnableMetrics();
    }

    public RaftNodeOptions enableMetrics(final boolean enableMetrics) {
        nodeOptions.setEnableMetrics(enableMetrics);
        return this;
    }

    public int cliRpcThreadPoolSize() {
        return nodeOptions.getCliRpcThreadPoolSize();
    }

    public RaftNodeOptions cliRpcThreadPoolSize(final int cliRpcThreadPoolSize) {
        nodeOptions.setCliRpcThreadPoolSize(cliRpcThreadPoolSize);
        return this;
    }

    public int raftRpcThreadPoolSize() {
        return nodeOptions.getRaftRpcThreadPoolSize();
    }

    public RaftNodeOptions raftRpcThreadPoolSize(final int raftRpcThreadPoolSize) {
        nodeOptions.setRaftRpcThreadPoolSize(raftRpcThreadPoolSize);
        return this;
    }

    public int timerPoolSize() {
        return nodeOptions.getTimerPoolSize();
    }

    public RaftNodeOptions timerPoolSize(final int timerPoolSize) {
        nodeOptions.setTimerPoolSize(timerPoolSize);
        return this;
    }

    public RaftOptions raftOptions() {
        return nodeOptions.getRaftOptions();
    }

    public RaftNodeOptions raftOptions(final RaftOptions raftOptions) {
        nodeOptions.setRaftOptions(raftOptions);
        return this;
    }

    public int electionPriority() {
        return nodeOptions.getElectionPriority();
    }

    public RaftNodeOptions electionPriority(int electionPriority) {
        nodeOptions.setElectionPriority(electionPriority);
        return this;
    }

    public int decayPriorityGap() {
        return nodeOptions.getDecayPriorityGap();
    }

    public RaftNodeOptions decayPriorityGap(int decayPriorityGap) {
        nodeOptions.setDecayPriorityGap(decayPriorityGap);
        return this;
    }

    public int electionTimeoutMs() {
        return nodeOptions.getElectionTimeoutMs();
    }

    public RaftNodeOptions electionTimeoutMs(final int electionTimeoutMs) {
        nodeOptions.setElectionTimeoutMs(electionTimeoutMs);
        return this;
    }

    public int leaderLeaseTimeRatio() {
        return nodeOptions.getLeaderLeaseTimeRatio();
    }

    public RaftNodeOptions leaderLeaseTimeRatio(final int leaderLeaseTimeRatio) {
        nodeOptions.setLeaderLeaseTimeRatio(leaderLeaseTimeRatio);
        return this;
    }

    public int leaderLeaseTimeoutMs() {
        return nodeOptions.getElectionTimeoutMs() * nodeOptions.getLeaderLeaseTimeRatio() / 100;
    }

    public int snapshotIntervalSecs() {
        return nodeOptions.getSnapshotIntervalSecs();
    }

    public RaftNodeOptions snapshotIntervalSecs(final int snapshotIntervalSecs) {
        nodeOptions.setSnapshotIntervalSecs(snapshotIntervalSecs);
        return this;
    }

    public int catchupMargin() {
        return nodeOptions.getCatchupMargin();
    }

    public RaftNodeOptions catchupMargin(final int catchupMargin) {
        nodeOptions.setCatchupMargin(catchupMargin);
        return this;
    }

    public Configuration initialConf() {
        return nodeOptions.getInitialConf();
    }

    public RaftNodeOptions initialConf(final Configuration initialConf) {
        nodeOptions.setInitialConf(initialConf);
        return this;
    }

    public StateMachine fsm() {
        return nodeOptions.getFsm();
    }

    public RaftNodeOptions fsm(final StateMachine fsm) {
        nodeOptions.setFsm(fsm);
        return this;
    }

    public String logUri() {
        return nodeOptions.getLogUri();
    }

    public RaftNodeOptions logUri(final String logUri) {
        nodeOptions.setLogUri(logUri);
        return this;
    }

    public String raftMetaUri() {
        return nodeOptions.getRaftMetaUri();
    }

    public RaftNodeOptions raftMetaUri(final String raftMetaUri) {
        nodeOptions.setRaftMetaUri(raftMetaUri);
        return this;
    }

    public String snapshotUri() {
        return nodeOptions.getSnapshotUri();
    }

    public RaftNodeOptions snapshotUri(final String snapshotUri) {
        nodeOptions.setSnapshotUri(snapshotUri);
        return this;
    }

    public boolean filterBeforeCopyRemote() {
        return nodeOptions.isFilterBeforeCopyRemote();
    }

    public RaftNodeOptions filterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        nodeOptions.setFilterBeforeCopyRemote(filterBeforeCopyRemote);
        return this;
    }

    public boolean disableCli() {
        return nodeOptions.isDisableCli();
    }

    public RaftNodeOptions disableCli(final boolean disableCli) {
        nodeOptions.setDisableCli(disableCli);
        return this;
    }

    public NodeOptions nodeOptions() {
        return nodeOptions;
    }

    public RaftNodeOptions nodeOptions(NodeOptions nodeOptions){
        this.nodeOptions = nodeOptions;
        return this;
    }

    @Override
    public String toString() {
        return "RaftNodeOptions{" +
                "nodeOptions=" + nodeOptions +
                '}';
    }
}
