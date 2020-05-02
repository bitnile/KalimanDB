package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.exception.StoreException;
import org.bitnile.kalimandb.common.operation.StoreOperation;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.storage.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author hecenjie
 */
public class StoreStateMachine extends StateMachineAdapter {

    private static final Logger logger = LoggerFactory.getLogger(StoreStateMachine.class);

    private final Store store;

    // Map to avoid repeat the same command to the state machine
    private final Cache<UUID, Object> cache;

    // Dummy value to associate with an key in cache
    private static final Object PRESENT = new Object();

    public StoreStateMachine(Store store, long cacheExpireTime, TimeUnit timeUnit) {
        this.store = store;
        this.cache = CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTime, timeUnit).build();
    }

    public StoreStateMachine(Store store, long cacheExpireTime, TimeUnit timeUnit, Ticker ticker) {
        this.store = store;
        this.cache = CacheBuilder.newBuilder().ticker(ticker).expireAfterAccess(cacheExpireTime, timeUnit).build();
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        Store store = this.store;
        String path = writer.getPath();
        try {
            store.writeSnapshot(path);
            done.run(Status.OK());
        } catch(StoreException ex){
            logger.error("Failed to save snapshot file, path={}", path);
            done.run(new Status(RaftError.EIO, "Failed to save snapshot file"));
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        Store store = this.store;
        String path = reader.getPath();
        try{
            store.readSnapshot(path);
            return true;
        } catch(StoreException ex){
            logger.error("Failed to load snapshot file, path={}", path);
            return false;
        }
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            StoreOperation storeOperation = null;
            StoreClosure closure = null;
            if(iter.done() != null){    // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (StoreClosure) iter.done();
                storeOperation = closure.getStoreOperation();
            } else{     // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    storeOperation = SerializerFactory.get(SerializerType.MESSAGE_PACK).read(data.array(), StoreOperation.class);
                } catch(Exception ex){
                    throw new RuntimeException("Failed to deserialize the storeOperation", ex);
                }
            }
            // The result of GET and GET_START_WITH operation still need to be replied to client
            if(storeOperation != null){
                switch(storeOperation.getOp()){
                    case StoreOperation.PUT:
                        applyPut(storeOperation.getKey(), storeOperation.getVal(), storeOperation.getCommandId(), closure);
                        break;
                    case StoreOperation.DELETE:
                        applyDelete(storeOperation.getKey(), storeOperation.getCommandId(), closure);
                        break;
                    case StoreOperation.GET:
                        applyGet(storeOperation.getKey(), closure);
                        break;
                    case StoreOperation.GET_START_WITH:
                        applyGetStartWith(storeOperation.getKey(), closure);
                        break;
                }
            }
            iter.next();
        }
    }

    private void applyPut(byte[] key, byte[] value, UUID commandId, StoreClosure closure){
        success(closure, null); // reply to client first
        try {
            if(cache.asMap().putIfAbsent(commandId, PRESENT) == null){ // If there is the same commandId
                store.put(key, value);
            }
        } catch(StoreException ex){
            throw new StoreException("Failed to PUT, error on Store", ex);
        }
    }

    private void applyDelete(byte[] key, UUID commandId, StoreClosure closure){
        success(closure, null); // reply to client first
        try {
            if(cache.asMap().putIfAbsent(commandId, PRESENT) == null){
                store.delete(key);
            }
        } catch(StoreException ex){
            throw new StoreException("Failed to DELETE, error on Store", ex);
        }
    }

    private void applyGet(byte[] key, StoreClosure closure){
        try {
            byte[] val = this.store.get(key);
            success(closure, val);
        } catch(StoreException ex){
            failure(closure, "Failed to GET, error on Store");
            throw new StoreException("Failed to GET, error on Store", ex);
        }
    }

    private void applyGetStartWith(byte[] prefix, StoreClosure closure){
        try {
            Map<byte[], byte[]> res = this.store.getStartWith(prefix);
            success(closure, res);
        } catch(StoreException ex){
            failure(closure, "Failed to GET_START_WITH, error on Store");
            throw new StoreException("Failed to GET_START_WITH, error on Store", ex);
        }
    }

    private void success(StoreClosure closure, Object data) {
        if(closure != null){    // closure is null on follower node
            closure.setData(data);
            closure.run(Status.OK());
        }
    }

    private void failure(StoreClosure closure, String msg){
        if(closure != null){    // closure is null on follower node
            closure.setError(ResponseCode.STORAGE_ERROR);
            closure.run(new Status(RaftError.EIO, msg));
        }
    }

    public Store getStore() {
        return store;
    }

    public Cache<UUID, Object> getCache() {
        return cache;
    }
}
