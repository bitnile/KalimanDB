package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import org.apache.commons.io.FileUtils;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.storage.RocksStore;
import org.bitnile.kalimandb.storage.option.StoreConfig;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RaftEngine extends LifecycleBase {
    private RaftGroupService raftGroupService;
    private RaftStore raftStore;
    private Node node;
    private StoreStateMachine fsm;

    public RaftEngine(RaftGroupService raftGroupService, RaftStore raftStore, Node node, StoreStateMachine fsm) {
        this.raftGroupService = raftGroupService;
        this.raftStore = raftStore;
        this.node = node;
        this.fsm = fsm;
    }

    public RaftEngine(final StoreConfig storeConfig, final RaftConfig raftConfig) throws IOException {
        RocksStore store = new RocksStore(storeConfig);
        this.fsm = new StoreStateMachine(store, raftConfig.cacheExpireTimeInMillis(), TimeUnit.MILLISECONDS);
        final RaftNodeOptions raftNodeOptions = raftConfig.raftNodeOptions();
        raftNodeOptions.fsm(this.fsm)
                .logUri(storeConfig.dbPath() + File.separator + "raft" + File.separator + "log")
                .raftMetaUri(storeConfig.dbPath() + File.separator + "raft" + File.separator + "raft_meta")
                .snapshotUri(storeConfig.dbPath() + File.separator + "raft" + File.separator + "snapshot");

        final String dbPath = store.dataPath();
        final File dbFile = new File(dbPath);
        FileUtils.deleteDirectory(dbFile);

        this.raftGroupService = new RaftGroupService(raftConfig.groupId(), raftConfig.serverId(), raftNodeOptions.nodeOptions());
        this.node = this.raftGroupService.start();
        this.raftStore = new RaftStore(this.node, store);
    }

    public RaftGroupService getRaftGroupService() {
        return raftGroupService;
    }

    public RaftStore getRaftStore() {
        return raftStore;
    }

    public Node getNode() {
        return node;
    }

    public StoreStateMachine getFsm() {
        return fsm;
    }

    protected void initInternal() {
        raftStore.init();
    }

    protected void startInternal() {
        raftStore.start();
    }

    protected void stopInternal() {
        raftStore.stop();
    }

    protected void destroyInternal() {
        raftStore.destroy();
    }
}
