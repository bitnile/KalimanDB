package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.google.common.collect.Maps;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.storage.RocksStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.UUID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RaftStoreTest {

    private RaftStore raftStore;

    @Before
    public void setUp() throws Exception {
        RocksStore rocksStore = mock(RocksStore.class);
        when(rocksStore.get(any())).thenReturn(null);
        when(rocksStore.getStartWith((any()))).thenReturn(Maps.newLinkedHashMap());
        Node node = mock(Node.class);
        when(node.isLeader()).thenReturn(true).thenReturn(false);   // first on leader, then on follower
        raftStore = new RaftStore(node, rocksStore);
        raftStore.init();
        raftStore.start();
    }

    @After
    public void tearDown() throws Exception {
        raftStore.stop();
        raftStore.destroy();
    }

    @Test
    public void put() {
        StoreClosure closure = mock(StoreClosure.class);
        raftStore.put("key0".getBytes(), "val0".getBytes(), UUID.randomUUID(), closure);   // apply on leader
        verify(raftStore.getNode()).apply(any());
        raftStore.put("key0".getBytes(), "val0".getBytes(), UUID.randomUUID(), closure);   // apply on follower
        verify(closure).setError(ResponseCode.NOT_LEADER);
        verify(closure).run(new Status(RaftError.ENEWLEADER, "Not leader"));
    }

    @Test
    public void get() {
        StoreClosure closure = mock(StoreClosure.class);
        raftStore.get("key0".getBytes(), UUID.randomUUID(), closure);
        verify(raftStore.getNode()).readIndex(any(), any());
    }

    @Test
    public void getStartWith() {
        StoreClosure closure = mock(StoreClosure.class);
        raftStore.getStartWith("key0_".getBytes(), UUID.randomUUID(), closure);
        verify(raftStore.getNode()).readIndex(any(), any());
    }

    @Test
    public void delete() {
        StoreClosure closure = mock(StoreClosure.class);
        raftStore.delete("key0".getBytes(), UUID.randomUUID(), closure);   // apply on leader
        verify(raftStore.getNode()).apply(any());
        raftStore.delete("key0".getBytes(), UUID.randomUUID(), closure);   // apply on follower
        verify(closure).setError(ResponseCode.NOT_LEADER);
        verify(closure).run(new Status(RaftError.ENEWLEADER, "Not leader"));
    }
}