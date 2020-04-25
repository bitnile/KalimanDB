package org.bitnile.kalimandb.service.kaliman.impl;

import org.bitnile.kalimandb.common.exception.ExecuteClosureException;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.service.kaliman.KalimanDBService;
import org.bitnile.kalimandb.service.option.ServiceConfig;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.raft.RaftStore;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.bitnile.kalimandb.common.document.impl.AbstractDocument.ID_NAME;

/**
 * @author ITcathyh
 */
public abstract class AbstractKalimanDBService extends LifecycleBase implements KalimanDBService {
    protected int maxWaitTime;
    protected int maxRetryTime;

    public AbstractKalimanDBService(ServiceConfig serviceConfig) {
        this.maxWaitTime = serviceConfig.maxWaitTime();
        this.maxRetryTime = serviceConfig.maxRetryTime();
    }

    public static Document checkDocument(Document document) {
        if (document == null) {
            throw new NullPointerException("document null");
        }

//        if (document.id() == null) {
//            document.append(ID_NAME, UUID.randomUUID());
//        }

        return document;
    }

    protected StoreClosure executeStoreClosure(RaftStoreAction action) throws ExecuteClosureException {
        CountDownLatch count = new CountDownLatch(1);
        StoreClosure storeClosure = new StoreClosure();
        storeClosure.setLatch(count);
        action.execute(storeClosure);

        try {
            if (!count.await(maxWaitTime, TimeUnit.SECONDS)) {
                throw new ExecuteClosureException("timeout");
            }
        } catch (InterruptedException e) {
            throw new ExecuteClosureException("interrupted");
        }

        if (!storeClosure.isSuccess()) {
            throw new ExecuteClosureException("executeStoreClosure err" + storeClosure.getError());
        }

        return storeClosure;
    }

    protected RaftStoreAction getPutAction(UUID commandId, RaftStore raftStore, final Document document) {
        return closure -> {
            raftStore.put(
                    SerializerFactory.get(SerializerType.STRING).write(document.valueOf(ID_NAME)),
                    SerializerFactory.get(SerializerType.MESSAGE_PACK).write(document),
                    commandId, closure);
        };
    }

    @FunctionalInterface
    interface RaftStoreAction {
        void execute(StoreClosure closure);
    }
}
