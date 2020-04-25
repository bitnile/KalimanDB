package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.common.base.Ticker;
import com.google.common.collect.Maps;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.operation.StoreOperation;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.storage.RocksStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class StoreStateMachineTest {

    private static final long EXPIRE_TIME = 10;
    private StoreStateMachine storeStateMachine;
    private Ticker testTicker;

    @Before
    public void setUp() throws Exception {
        RocksStore rocksStore = mock(RocksStore.class);
        when(rocksStore.get(any())).thenReturn(null);
        when(rocksStore.getStartWith((any()))).thenReturn(Maps.newLinkedHashMap());
        testTicker = new TestTicker();
        storeStateMachine = new StoreStateMachine(rocksStore, EXPIRE_TIME, TimeUnit.SECONDS, testTicker);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testOnSnapshotSave() {
        Closure done = (final Status status)-> assertTrue(status.isOk());
        SnapshotWriter writer = mock(SnapshotWriter.class);
        when(writer.getPath()).thenReturn("./snapshot");
        storeStateMachine.onSnapshotSave(writer, done);
        verify(writer, atLeastOnce()).getPath();
        verify((RocksStore)storeStateMachine.getStore()).writeSnapshot(writer.getPath());
    }

    @Test
    public void testOnSnapshotLoad() {
        SnapshotReader reader = mock(SnapshotReader.class);
        when(reader.getPath()).thenReturn("./snapshot");
        assertTrue(storeStateMachine.onSnapshotLoad(reader));
        verify(reader, atLeastOnce()).getPath();
        verify((RocksStore)storeStateMachine.getStore()).readSnapshot(reader.getPath());
    }

    @Test
    public void testOnApplyLeader() {
        // PUT
        StoreClosure closure = mock(StoreClosure.class);
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.PUT, "key0".getBytes(), "val0".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation);
        Iterator iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.done()).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).put("key0".getBytes(), "val0".getBytes());

        // DELETE
        closure = mock(StoreClosure.class);
        storeOperation = StoreOperation.newOperation(StoreOperation.DELETE, "key0".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation);
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.done()).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).delete("key0".getBytes());

        // GET
        closure = mock(StoreClosure.class);
        storeOperation = StoreOperation.newOperation(StoreOperation.GET, "key0".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation);
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.done()).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).get("key0".getBytes());
        verify(closure).setData(storeStateMachine.getStore().get("key0".getBytes()));
        verify(closure).run(Status.OK());

        // GET_START_WITH
        closure = mock(StoreClosure.class);
        storeOperation = StoreOperation.newOperation(StoreOperation.GET_START_WITH, "key0_".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation);
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.done()).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).getStartWith("key0_".getBytes());
        verify(closure).setData(storeStateMachine.getStore().getStartWith("key0_".getBytes()));
        verify(closure).run(Status.OK());
    }

    @Test
    public void testOnApplyFollower() throws Exception {
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.PUT, "key0".getBytes(), "val0".getBytes(), UUID.randomUUID());
        ByteBuffer data = ByteBuffer.wrap(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(storeOperation));
        Iterator iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.getData()).thenReturn(data);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).put("key0".getBytes(), "val0".getBytes());

        storeOperation = StoreOperation.newOperation(StoreOperation.DELETE, "key0".getBytes(), UUID.randomUUID());
        data = ByteBuffer.wrap(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(storeOperation));
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.getData()).thenReturn(data);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).delete("key0".getBytes());

        storeOperation = StoreOperation.newOperation(StoreOperation.GET, "key0".getBytes(), UUID.randomUUID());
        data = ByteBuffer.wrap(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(storeOperation));
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.getData()).thenReturn(data);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).get("key0".getBytes());

        storeOperation = StoreOperation.newOperation(StoreOperation.GET_START_WITH, "key0_".getBytes(), UUID.randomUUID());
        data = ByteBuffer.wrap(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(storeOperation));
        iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(false);
        when(iter.getData()).thenReturn(data);
        storeStateMachine.onApply(iter);
        verify(storeStateMachine.getStore()).getStartWith("key0_".getBytes());
    }

    @Test
    public void testPutWithSameCommandId(){
        StoreClosure closure = mock(StoreClosure.class);
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.PUT, "key0".getBytes(), "val0".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation).thenReturn(storeOperation);
        Iterator iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);   // apply the same operation twice
        when(iter.done()).thenReturn(closure).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(closure, times(2)).run(Status.OK());
        verify(storeStateMachine.getStore(), times(1)).put("key0".getBytes(), "val0".getBytes());
        assertNotNull(storeStateMachine.getCache().getIfPresent(storeOperation.getCommandId()));

        // simulate the elapsed time
        ((TestTicker)testTicker).addElapsedTime(TimeUnit.NANOSECONDS.convert(EXPIRE_TIME, TimeUnit.SECONDS));
        assertNull(storeStateMachine.getCache().getIfPresent(storeOperation.getCommandId()));
    }

    @Test
    public void testDeleteWithSameCommandId() throws InterruptedException {
        StoreClosure closure = mock(StoreClosure.class);
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.DELETE, "key0".getBytes(), UUID.randomUUID());
        when(closure.getStoreOperation()).thenReturn(storeOperation).thenReturn(storeOperation);
        Iterator iter = mock(Iterator.class);
        when(iter.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);   // apply the same operation twice
        when(iter.done()).thenReturn(closure).thenReturn(closure);
        storeStateMachine.onApply(iter);
        verify(closure, times(2)).run(Status.OK());
        verify(storeStateMachine.getStore(), times(1)).delete("key0".getBytes());
        assertNotNull(storeStateMachine.getCache().getIfPresent(storeOperation.getCommandId()));

        // simulate the elapsed time
        ((TestTicker)testTicker).addElapsedTime(TimeUnit.NANOSECONDS.convert(EXPIRE_TIME, TimeUnit.SECONDS));
        assertNull(storeStateMachine.getCache().getIfPresent(storeOperation.getCommandId()));
    }


    private static class TestTicker extends Ticker {
        private long start = Ticker.systemTicker().read();
        private long elapsedNano = 0;

        @Override
        public long read() {
            return start + elapsedNano;
        }

        public void addElapsedTime(long elapsedNano) {
            this.elapsedNano = elapsedNano;
        }
    }
}