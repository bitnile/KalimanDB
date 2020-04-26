package org.bitnile.kalimandb.service.kaliman.impl;

import org.bitnile.kalimandb.service.KalimanDBServiceImpl;
import org.bitnile.kalimandb.service.ServerRemotingProcessor;
import org.bitnile.kalimandb.service.status.DBOperationStatus;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.DefaultDocument;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.common.protocol.ServiceEnum;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceResult;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ServerRemotingProcessorTest {
    private static final Logger logger = LoggerFactory.getLogger(ServerRemotingProcessorTest.class);

    private static final Serializer msgpackSerializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);

    ServerRemotingProcessor processor;

    KalimanDBServiceImpl service;

    @Before
    public void setUp() {
        service = mock(KalimanDBServiceImpl.class);

        processor = new ServerRemotingProcessor(service);

        when(service.find(any(), any())).thenReturn(CompositeResult.builder().msg("Find Succ")
                .status(DBOperationStatus.SUCCESS)
                .build());

        when(service.findStartWith(any(), any())).thenReturn(CompositeResult.builder().msg("FindStartWith Succ")
                .status(DBOperationStatus.SUCCESS)
                .build());

        when(service.insert(any(), any())).thenReturn(Result.builder().msg("Insert Succ")
                .status(DBOperationStatus.SUCCESS)
                .build());

        when(service.update(any(), any())).thenReturn(Result.builder().msg("Update Succ")
                .status(DBOperationStatus.SUCCESS)
                .build());

        when(service.delete(any(), any())).thenReturn(Result.builder().msg("Delete Succ")
                .status(DBOperationStatus.SUCCESS)
                .build());
    }


    @Test
    public void insert() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        UUID uuid = UUID.randomUUID();
        header.setMethod(ServiceEnum.INSERT.getCode());
        header.setLeastSignificantBits(uuid.getLeastSignificantBits());
        header.setMostSignificantBits(uuid.getMostSignificantBits());

        Document document = new DefaultDocument().append("aa", "bb");
        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        args.setArgs(Collections.singletonList(document));

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setHeaderBytes(msgpackSerializer.write(header));
        request.setBody(msgpackSerializer.write(args));
        RPCPacket response = processor.databaseService(request);

//        logger.debug(response.toString());

        DatabaseServiceResult result = msgpackSerializer.read(response.getBody(), DatabaseServiceResult.class);

        assertEquals(result.getStatus(), 0);
        assertEquals(result.getMsg(), "Insert Succ");
    }

    @Test
    public void Update() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        UUID uuid = UUID.randomUUID();
        header.setMethod(ServiceEnum.UPDATE.getCode());
        header.setLeastSignificantBits(uuid.getLeastSignificantBits());
        header.setMostSignificantBits(uuid.getMostSignificantBits());

        Document document = new DefaultDocument().append("aa", "bb");
        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        args.setArgs(Collections.singletonList(document));

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setHeaderBytes(msgpackSerializer.write(header));
        request.setBody(msgpackSerializer.write(args));
        RPCPacket response = processor.databaseService(request);

        logger.debug(response.toString());

        DatabaseServiceResult result = msgpackSerializer.read(response.getBody(), DatabaseServiceResult.class);

        assertEquals(result.getStatus(), 0);
        assertEquals(result.getMsg(), "Update Succ");

    }

    @Test
    public void deleteTest() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        UUID uuid = UUID.randomUUID();
        header.setMethod(ServiceEnum.DELETE.getCode());
        header.setLeastSignificantBits(uuid.getLeastSignificantBits());
        header.setMostSignificantBits(uuid.getMostSignificantBits());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        args.setArgs(Collections.singletonList("deleteId"));

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setHeaderBytes(msgpackSerializer.write(header));
        request.setBody(msgpackSerializer.write(args));
        RPCPacket response = processor.databaseService(request);

        logger.debug(response.toString());

        DatabaseServiceResult result = msgpackSerializer.read(response.getBody(), DatabaseServiceResult.class);

        assertEquals(result.getStatus(), 0);
        assertEquals(result.getMsg(), "Delete Succ");
    }

    @Test
    public void find() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        UUID uuid = UUID.randomUUID();
        header.setMethod(ServiceEnum.FIND.getCode());
        header.setLeastSignificantBits(uuid.getLeastSignificantBits());
        header.setMostSignificantBits(uuid.getMostSignificantBits());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        args.setArgs(Collections.singletonList("find Id"));

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setHeaderBytes(msgpackSerializer.write(header));
        request.setBody(msgpackSerializer.write(args));
        RPCPacket response = processor.databaseService(request);

        logger.debug(response.toString());

        DatabaseServiceResult result = msgpackSerializer.read(response.getBody(), DatabaseServiceResult.class);

        assertEquals(result.getStatus(), 0);
        assertEquals(result.getMsg(), "Find Succ");
    }

    @Test
    public void findStartWith() throws Exception {
        DatabaseServiceHeader header = new DatabaseServiceHeader();
        UUID uuid = UUID.randomUUID();
        header.setMethod(ServiceEnum.FIND_STW.getCode());
        header.setLeastSignificantBits(uuid.getLeastSignificantBits());
        header.setMostSignificantBits(uuid.getMostSignificantBits());

        DatabaseServiceRequestArgs args = new DatabaseServiceRequestArgs();
        args.setArgs(Collections.singletonList("find Id"));

        RPCPacket request = RPCPacket.createRequestPacket(RequestCode.DATABASE_SERVICE, header);
        request.setHeaderBytes(msgpackSerializer.write(header));
        request.setBody(msgpackSerializer.write(args));
        RPCPacket response = processor.databaseService(request);

        logger.debug(response.toString());

        DatabaseServiceResult result = msgpackSerializer.read(response.getBody(), DatabaseServiceResult.class);

        assertEquals(result.getStatus(), 0);
        assertEquals(result.getMsg(), "FindStartWith Succ");
    }
}