package org.bitnile.kalimandb.service;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.service.option.ServiceConfig;
import org.bitnile.kalimandb.service.status.DBOperationStatus;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.DefaultDocument;
import org.bitnile.kalimandb.raft.RaftEngine;
import org.bitnile.kalimandb.raft.RaftStore;
import org.bitnile.kalimandb.raft.StoreStateMachine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.bitnile.kalimandb.common.document.AbstractDocument.ID_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class KalimanDBServiceTest {
    private KalimanDBServiceImpl service;

    static Document getDocument() {
        return new DefaultDocument().append(ID_NAME, "test").append("a", "b");
    }

    @Before
    public void start() {
        RaftGroupService raftGroupService = mock(RaftGroupService.class);
        RaftStore raftStore = mock(RaftStore.class);
        Node node = mock(Node.class);
        StoreStateMachine fsm = mock(StoreStateMachine.class);
        RaftEngine raftEngine = new RaftEngine(raftGroupService, raftStore, node, fsm);
        ServiceConfig opt = new ServiceConfig();

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            invocation.getMock();

            StoreClosure storeClosure = (StoreClosure) args[args.length - 1];
            storeClosure.getLatch().countDown();
            storeClosure.setData(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(getDocument()));
            return null;
        }).when(raftStore).get(any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            invocation.getMock();

            StoreClosure storeClosure = (StoreClosure) args[args.length - 1];
            storeClosure.getLatch().countDown();
            return null;
        }).when(raftStore).put(any(), any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            invocation.getMock();

            StoreClosure storeClosure = (StoreClosure) args[args.length - 1];
            storeClosure.getLatch().countDown();
            return null;
        }).when(raftStore).delete(any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            invocation.getMock();

            StoreClosure storeClosure = (StoreClosure) args[args.length - 1];
            storeClosure.getLatch().countDown();
            Document document = getDocument();
            Map<byte[], byte[]> map = new LinkedHashMap<>();

            map.put(SerializerFactory.get(SerializerType.STRING).write(document.valueOf(ID_NAME)),
                    SerializerFactory.get(SerializerType.MESSAGE_PACK).write(document));
            storeClosure.setData(map);
            return null;
        }).when(raftStore).getStartWith(any(), any(), any());

        service = new KalimanDBServiceImpl(raftEngine, opt);
        service.init();
        service.start();
    }

    @After
    public void finish() throws Exception {
        service.stop();
        service.destroy();
    }

    @Test
    public void testInsert() throws Exception {
        UUID uuid = UUID.randomUUID();
        Result res = service.insert(uuid, getDocument());
        assertEquals(res.getStatus(), DBOperationStatus.SUCCESS);
        assertNotNull(res.getDocument());
        assertEquals(res.getDocument(), getDocument());
    }

    @Test
    public void testFind() throws Exception {
        UUID uuid = UUID.randomUUID();
        CompositeResult res = service.find(uuid, "test");
        assertEquals(res.getStatus(), DBOperationStatus.SUCCESS);
        assertNotNull(res.getDocuments());
        assertEquals(res.getDocuments().length, 1);
        assertEquals(res.getDocuments()[0], getDocument());
    }

    @Test
    public void testDelete() throws Exception {
        UUID uuid = UUID.randomUUID();
        Result res = service.delete(uuid, "test");
        assertEquals(res.getStatus(), DBOperationStatus.SUCCESS);
        assertNotNull(res.getDocument());
        assertEquals(res.getDocument().id(), getDocument().id());
    }

    @Test
    public void testUpdate() throws Exception {
        UUID uuid = UUID.randomUUID();
        Result res = service.update(uuid, getDocument());
        assertEquals(res.getStatus(), DBOperationStatus.SUCCESS);
        assertNotNull(res.getDocument());
        assertEquals(res.getDocument(), getDocument());
    }

    @Test
    public void testFindStartWith() throws Exception {
        UUID uuid = UUID.randomUUID();
        CompositeResult res = service.findStartWith(uuid, "1");
        assertEquals(res.getStatus(), DBOperationStatus.SUCCESS);
        assertNotNull(res.getDocuments());
        assertEquals(res.getDocuments().length, 1);
        assertEquals(res.getDocuments()[0], getDocument());
    }

}
