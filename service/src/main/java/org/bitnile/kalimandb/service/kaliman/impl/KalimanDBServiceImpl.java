package org.bitnile.kalimandb.service.kaliman.impl;

import org.bitnile.kalimandb.common.document.impl.DefaultDocument;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.service.option.ServiceConfig;
import org.bitnile.kalimandb.service.status.DBOperationStatus;
import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.raft.RaftEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static org.bitnile.kalimandb.common.document.impl.AbstractDocument.ID_NAME;

/**
 * @author ITcathyh
 */
public class KalimanDBServiceImpl extends AbstractKalimanDBService {
    private static final Logger log = LoggerFactory.getLogger(KalimanDBServiceImpl.class);
    private RaftEngine raftEngine;
//    private ExecutorService executor;
//    private RetryTaskManager retryTaskManager;

    public KalimanDBServiceImpl(RaftEngine raftEngine, ServiceConfig serviceConfig) {
        super(serviceConfig);
        this.raftEngine = raftEngine;
//        executor = ExecutorServiceFactory.newThreadPool("KalimanDBImplThread");
    }

    @Override
    public Result insert(UUID commandId, Document document) {
        document = checkDocument(document);

        try {
            executeStoreClosure(getPutAction(commandId, this.raftEngine.getRaftStore(), document));
        } catch (Exception e) {
            log.error("insert err", e);
            return Result.builder()
                    .msg("insert err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        return Result.builder()
                .status(DBOperationStatus.SUCCESS)
                .document(document)
                .build();
    }

    @Override
    public Result update(UUID commandId, Document document) {
        if (document.id() == null) {
            return Result.builder()
                    .msg("invalid document")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        try {
            executeStoreClosure(getPutAction(commandId, this.raftEngine.getRaftStore(), document));
        } catch (Exception e) {
            log.error("update err", e);
            return Result.builder()
                    .msg("update err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        return Result.builder()
                .status(DBOperationStatus.SUCCESS)
                .document(document)
                .build();
    }

    @Override
    public Result delete(UUID commandId, Object id) {
        RaftStoreAction action = closure -> {
            this.raftEngine.getRaftStore()
                    .delete(SerializerFactory.get(SerializerType.STRING).write(id), commandId, closure);
        };

        try {
            executeStoreClosure(action);
        } catch (Exception e) {
            log.error("delete err", e);
            return Result.builder()
                    .msg("delete err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        Document document = new DefaultDocument().append(ID_NAME, id);    // TODO(hecenjie)

        return Result.builder()
                .status(DBOperationStatus.SUCCESS)
                .document(document)
                .build();
    }

    @Override
    public CompositeResult find(UUID commandId, Object id) {
        StoreClosure storeClosure;
        RaftStoreAction action = closure -> {
            try {
                this.raftEngine.getRaftStore().get(
                        SerializerFactory.get(SerializerType.STRING).write(id), commandId, closure);
            } catch (Exception e) {
                throw new RuntimeException("find error", e);
            }
        };

        try {
            storeClosure = executeStoreClosure(action);
        } catch (Exception e) {
            log.error("find err", e);
            return CompositeResult.builder()
                    .msg("find err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        Object data = storeClosure.getData();

        Document[] documents;

        if(data == null) {
            documents = new Document[0];
            return CompositeResult.builder()
                    .status(DBOperationStatus.SUCCESS)
                    .documents(documents)
                    .build();
        } else if (data instanceof byte[]) {
            documents = new Document[1];
            Document document;
            document = SerializerFactory.get(SerializerType.MESSAGE_PACK).read((byte[])data, DefaultDocument.class);
            documents[0] = document;
            return CompositeResult.builder()
                    .status(DBOperationStatus.SUCCESS)
                    .documents(documents)
                    .build();
        }

        log.error("find error");
        return CompositeResult.builder()
                .msg("find error")
                .status(DBOperationStatus.FAILURE)
                .build();
    }

    @Override
    public CompositeResult findStartWith(UUID commandId, Object prefix) {
        StoreClosure storeClosure;
        RaftStoreAction action = closure -> {
            this.raftEngine.getRaftStore().getStartWith(
                    SerializerFactory.get(SerializerType.STRING).write(prefix), commandId, closure);
        };

        try {
            storeClosure = executeStoreClosure(action);
        } catch (Exception e) {
            log.error("findStartWith err", e);
            return CompositeResult.builder()
                    .msg("find err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }

        if (!(storeClosure.getData() instanceof Map)) {
            log.error("findStartWith err");
            return CompositeResult.builder()
                    .msg("find err")
                    .status(DBOperationStatus.FAILURE)
                    .build();
        }
        // Map: id -> document
        Map<byte[], byte[]> map = (Map<byte[], byte[]>) storeClosure.getData();
        Document[] documents = new Document[map.size()];
        int index = 0;
        for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
            Document document;
            document = SerializerFactory.get(SerializerType.MESSAGE_PACK).read(entry.getValue(), DefaultDocument.class);
            documents[index++] = document;
        }

        return CompositeResult.builder()
                .status(DBOperationStatus.SUCCESS)
                .documents(documents)
                .build();
    }

    protected void initInternal() {
        raftEngine.init();
    }

    protected void startInternal() {
        raftEngine.start();
    }

    protected void stopInternal() {
        raftEngine.stop();
    }

    protected void destroyInternal() {
        raftEngine.destroy();
    }
}
