package org.bitnile.kalimandb.client.factory;

import com.alipay.sofa.jraft.entity.PeerId;
import org.bitnile.kalimandb.service.status.DBOperationStatus;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.client.DatabaseInstanceManager;
import org.bitnile.kalimandb.client.KalimanDBClientRemotingService;
import org.bitnile.kalimandb.client.kaliman.KalimanDB;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.DefaultDocument;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceResult;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.common.utils.DocumentUtils;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DefaultKalimanDBFactoryTest {
    private static final Serializer msgpackSerializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);

    @Test
    public void parseResponse() throws Exception {
        Document document = new DefaultDocument()
                .append("aa", "bb")
                .append("cc", "dd");

        Document document2 = new DefaultDocument()
                .append("ee", "ff")
                .append("gg", "hh");

        Document[] documents = new Document[]{document, document2};

        RPCPacket response = RPCPacket.createResponsePacket(0,  null);
        DatabaseServiceResult result = new DatabaseServiceResult(0, "test msg", 123, DocumentUtils.documents2Bytes(documents));
        response.setBody(msgpackSerializer.write(result));

        DefaultKalimanDBFactory factory = new DefaultKalimanDBFactory();
        Result compositeResult = (Result) factory.parseResponse("update", response);

        assertEquals(compositeResult.getStatus(), DBOperationStatus.valueOf(0));
        assertEquals(compositeResult.getMsg(), "test msg");
        assertEquals(compositeResult.getErrCode(), 123);

        assertEquals(compositeResult.getDocument(), document);

        CompositeResult getOperationResult = (CompositeResult) factory.parseResponse("find", response);
        assertEquals(getOperationResult.getStatus(), DBOperationStatus.valueOf(0));
        assertEquals(getOperationResult.getMsg(), "test msg");
        assertEquals(getOperationResult.getErrCode(), 123);
        assertEquals(getOperationResult.getDocuments()[0], document);
        assertEquals(getOperationResult.getDocuments()[1], document2);
    }

    @Test
    public void getKalimanDB() throws Exception {
        Document document = new DefaultDocument()
                .append("aa", "bb")
                .append("cc", "dd");

        Document document2 = new DefaultDocument()
                .append("ee", "ff")
                .append("gg", "hh");

        Document[] documents = new Document[]{document, document2};

        RPCPacket response = RPCPacket.createResponsePacket(0,  null);
        DatabaseServiceResult result = new DatabaseServiceResult(0, "test msg", 123, DocumentUtils.documents2Bytes(documents));
        response.setBody(msgpackSerializer.write(result));

        KalimanDBClientRemotingService service = mock(KalimanDBClientRemotingService.class);
        when(service.sendRequest(anyString(), any(), any(), anyBoolean())).thenReturn(response);

        DatabaseInstanceManager manager = mock(DatabaseInstanceManager.class);
        when(manager.selectLeader()).thenReturn(new PeerId("127.0.0.1", 8899));
        when(manager.getServiceAddress("127.0.0.1:8899")).thenReturn("127.0.0.1:9001");

        KalimanDBFactory factory = new DefaultKalimanDBFactory(manager, service);

        KalimanDB kalimanDB = factory.getKalimanDB();

        CompositeResult getOperationResult = kalimanDB.find("1");
        assertEquals(getOperationResult.getMsg(), "test msg");
        assertEquals(getOperationResult.getErrCode(), 123);
        assertEquals(getOperationResult.getDocuments()[0], document);
        assertEquals(getOperationResult.getDocuments()[1], document2);


        Result compositeResult = kalimanDB.update(document);
        assertEquals(compositeResult.getErrCode(), 123);
        assertEquals(compositeResult.getMsg(), "test msg");
        assertEquals(compositeResult.getDocument(), document);
    }
}