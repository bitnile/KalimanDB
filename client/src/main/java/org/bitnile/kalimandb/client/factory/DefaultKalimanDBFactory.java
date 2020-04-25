package org.bitnile.kalimandb.client.factory;

import com.alipay.sofa.jraft.entity.PeerId;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.impl.DefaultDocument;
import org.bitnile.kalimandb.service.status.DBOperationStatus;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.client.DatabaseInstanceManager;
import org.bitnile.kalimandb.client.kaliman.KalimanDB;
import org.bitnile.kalimandb.client.KalimanDBClientRemotingService;
import org.bitnile.kalimandb.common.exception.DatabaseClientException;
import org.bitnile.kalimandb.common.protocol.KalimanDBFunction;
import org.bitnile.kalimandb.common.protocol.ServiceEnum;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceResult;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.TimeoutException;


public class DefaultKalimanDBFactory implements KalimanDBFactory {
    private static final Logger logger = LoggerFactory.getLogger(DefaultKalimanDBFactory.class);
    private static final Serializer msgpackSerializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);

    private DatabaseInstanceManager instanceManager;
    private KalimanDBClientRemotingService remotingService;

    private KalimanDB kalimanDB;

    public DefaultKalimanDBFactory(DatabaseInstanceManager instanceManager, KalimanDBClientRemotingService remotingService) {
        this.instanceManager = instanceManager;
        this.remotingService = remotingService;
    }

    public DefaultKalimanDBFactory() {
    }

    @Override
    public KalimanDB getKalimanDB() {
        if (kalimanDB == null) {
            kalimanDB = (KalimanDB) Proxy.newProxyInstance(KalimanDB.class.getClassLoader(), new Class[]{KalimanDB.class}, new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws DatabaseClientException {
                    DatabaseServiceHeader header = new DatabaseServiceHeader();
                    UUID uuid = UUID.randomUUID();
                    header.setMethod(ServiceEnum.name2Code(method.getName()));
                    header.setLeastSignificantBits(uuid.getLeastSignificantBits());
                    header.setMostSignificantBits(uuid.getMostSignificantBits());

                    DatabaseServiceRequestArgs requestArgs = new DatabaseServiceRequestArgs();

                    List<Object> argsList = new ArrayList<>();
                    for (Object arg : args) {
                        if (arg instanceof Document) {
                            argsList.add(((Document) arg).getContent());
                        } else {
                            argsList.add(arg);
                        }
                    }

                    requestArgs.setArgs(argsList);

                    PeerId leader = null;
                    try {
                        leader = instanceManager.selectLeader();
                    } catch (TimeoutException | InterruptedException e) {
                        logger.error(instanceManager.getGroupId() + ", " + instanceManager.getRaftAddr() + " leader not found");
                    }

                    if (leader == null) {
                        throw new DatabaseClientException(instanceManager.getGroupId(), instanceManager.getRaftAddr() + " leader not found");
                    }

                    String raftAddr = leader.getIp() + ":" + leader.getPort();
                    logger.info("Find raft leader address: " + raftAddr);

                    String serviceAddr = instanceManager.getServiceAddress(raftAddr);
                    logger.info("Find service leader address: " + serviceAddr);

                    RPCPacket response = null;
                    response = remotingService.sendRequest(serviceAddr, header, requestArgs,  true);

                    if (response != null) {
                        return parseResponse(method.getName(), response);
                    }

                    throw new DatabaseClientException();
                }
            });
        }


        return kalimanDB;
    }

    public Object parseResponse(String methodName, RPCPacket response) {
        byte[] body = response.getBody();

        DatabaseServiceResult result = null;
        try {
            result = msgpackSerializer.read(body, DatabaseServiceResult.class);
        } catch (Exception e) {
            logger.error("DatabaseServiceResult decode error", e);
        }

        if (result == null) {
            logger.error("Response is received, but result is empty");
            return null;
        }


        if (KalimanDBFunction.INSERT_METHOD.equals(methodName) || KalimanDBFunction.UPDATE_METHOD.equals(methodName) || KalimanDBFunction.DELETE_METHOD.equals(methodName)) {
            Map resultMap = null;
            try {
                resultMap = msgpackSerializer.read(result.getDocumentsBytes()[0], LinkedHashMap.class);
            } catch (Exception e) {
                logger.error("Result Map decode error", e);
            }

            Result result1 = Result.builder()
                    .msg(result.getMsg())
                    .document(new DefaultDocument(resultMap))
                    .errCode(result.getCode())
                    .status(DBOperationStatus.valueOf(result.getStatus()))
                    .build();
            logger.debug("Document in result of [INSERT, UPDATE, DELETE] : " + result1.getDocument());
            return result1;
        } else if (KalimanDBFunction.FIND_METHOD.equals(methodName) || KalimanDBFunction.FIND_START_WITH_METHOD.equals(methodName)) {
            Document[] resultDocuments;
            if(result.getDocumentsBytes() != null && result.getDocumentsBytes().length != 0) {
                resultDocuments = new Document[result.getDocumentsBytes().length];
                try {
                    for (int i = 0; i < result.getDocumentsBytes().length; i++) {
                        LinkedHashMap content = msgpackSerializer.read(result.getDocumentsBytes()[i], LinkedHashMap.class);
                        resultDocuments[i] = new DefaultDocument(content);
                    }
                } catch (Exception e) {
                    logger.error("Result Map decode error", e);
                }
                CompositeResult result1 = CompositeResult.builder()
                        .msg(result.getMsg())
                        .documents(resultDocuments)
                        .errCode(result.getCode())
                        .status(DBOperationStatus.valueOf(result.getStatus()))
                        .build();
                logger.debug("Document (the first element) in result of [FIND, FIND_START_WITH] : " + result1.getDocuments()[0]);
                return result1;
            } else {    // maybe find nothing
                resultDocuments = new Document[0];
                CompositeResult result1 = CompositeResult.builder()
                        .msg(result.getMsg())
                        .documents(resultDocuments)
                        .errCode(result.getCode())
                        .status(DBOperationStatus.valueOf(result.getStatus()))
                        .build();
                logger.debug("Document (the first element) in result of [FIND, FIND_START_WITH] : nothing");
                return result1;
            }
        }
        return null;
    }
}
