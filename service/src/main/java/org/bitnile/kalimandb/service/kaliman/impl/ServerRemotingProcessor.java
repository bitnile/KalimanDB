package org.bitnile.kalimandb.service.kaliman.impl;

import io.netty.channel.ChannelHandlerContext;
import org.bitnile.kalimandb.common.document.impl.DefaultDocument;
import org.bitnile.kalimandb.service.kaliman.KalimanDBService;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.protocol.RequestCode;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.common.protocol.ServiceEnum;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceRequestArgs;
import org.bitnile.kalimandb.common.protocol.body.DatabaseServiceResult;
import org.bitnile.kalimandb.common.protocol.header.DatabaseServiceHeader;
import org.bitnile.kalimandb.common.serializer.Serializer;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.common.utils.DocumentUtils;
import org.bitnile.kalimandb.rpc.netty.NettyRequestProcessor;
import org.bitnile.kalimandb.rpc.protocol.PacketMsgPackSerializer;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.UUID;


public class ServerRemotingProcessor implements NettyRequestProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ServerRemotingProcessor.class);

    private static final Serializer msgPackSerializer = SerializerFactory.get(SerializerType.MESSAGE_PACK);
    private final KalimanDBService kalimanDBService;

    public ServerRemotingProcessor(KalimanDBService kalimanDBService) {
        this.kalimanDBService = kalimanDBService;
    }

    @Override
    public RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.DATABASE_SERVICE:
                return databaseService(request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RPCPacket databaseService(RPCPacket request) {
        final DatabaseServiceHeader serviceHeader = (DatabaseServiceHeader) PacketMsgPackSerializer.
                decodeCustomHeader(request.getHeaderBytes(), DatabaseServiceHeader.class);

        Method[] serviceMethod = kalimanDBService.getClass().getMethods();
        DatabaseServiceRequestArgs args = null;

        try {
            args = msgPackSerializer.read(request.getBody(), DatabaseServiceRequestArgs.class);
        } catch (Exception e) {
            logger.error("DatabaseServiceRequestArgs decode error", e);
        }

        if (args == null) {
            return RPCPacket.createResponsePacket(ResponseCode.ARGS_NULL, "No parameters error");
        }

        UUID commandId = new UUID(serviceHeader.getMostSignificantBits(), serviceHeader.getLeastSignificantBits());

        LinkedList<Object> argsList = new LinkedList<>();

        for (Object arg : args.getArgs()) {
            // document from client to server will deserialize to LinkedHashMap
            if (arg instanceof LinkedHashMap) {
                Document document = new DefaultDocument((LinkedHashMap)arg);
                argsList.add(document);
                continue;
            }
            argsList.add(arg);
        }

        // the parameter: UUID commandId, Document document
        // put the UUID to first
        argsList.addFirst(commandId);
        Object result = null;

        String requestMethod = ServiceEnum.valueOf(serviceHeader.getMethod()).getName();

        for (Method method : serviceMethod) {
            if (method.getName().equals(requestMethod)) {
                try {
                    result = method.invoke(kalimanDBService, argsList.toArray());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    logger.error("method invoke error", e);
                    return RPCPacket.createResponsePacket(ResponseCode.SYSTEM_ERROR, "method invoke error" + e);
                } catch (Exception e) {
                    logger.error("Some unknown error", e);
                    return RPCPacket.createResponsePacket(ResponseCode.SYSTEM_ERROR, "method invoke error" + e);
                }
                break;
            }
        }



        if (result == null) {
            return RPCPacket.createResponsePacket(ResponseCode.NO_METHOD_SUPPORT, "no method supported");
        } else {
            RPCPacket resp = RPCPacket.createResponsePacket(ResponseCode.SUCCESS, null);
            DatabaseServiceResult serviceResult = new DatabaseServiceResult();

            if (result instanceof Result) {
                serviceResult.setCode(((Result) result).getErrCode());

                if (((Result) result).getDocument() != null) {
                    try {
                        serviceResult.setDocumentsBytes(new byte[][]{msgPackSerializer.write(((Result) result).getDocument().getContent())});
                    } catch (Exception e) {
                        logger.error("Result document encode error", e);
                    }
                }

                serviceResult.setMsg(((Result) result).getMsg());
                serviceResult.setStatus(((Result) result).getStatus().getStatus());

            } else if (result instanceof CompositeResult) {
                serviceResult.setCode(((CompositeResult) result).getErrCode());

                if (((CompositeResult) result).getDocuments() != null) {
                    try {
                        serviceResult.setDocumentsBytes(DocumentUtils.documents2Bytes(((CompositeResult) result).getDocuments()));
                    } catch (Exception e) {
                        logger.error("Result documents encode error", e);
                    }
                }

                serviceResult.setMsg(((CompositeResult) result).getMsg());
                serviceResult.setStatus(((CompositeResult) result).getStatus().getStatus());
            }

            byte[] body = null;

            try {
                body = msgPackSerializer.write(serviceResult);
            } catch (Exception e) {
                logger.error("body encode error", e);
            }

            resp.setBody(body);

            return resp;
        }
    }
}
