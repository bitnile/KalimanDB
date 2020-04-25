package org.bitnile.kalimandb.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import org.bitnile.kalimandb.common.LifecycleBase;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.exception.StoreException;
import org.bitnile.kalimandb.common.operation.StoreOperation;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;
import org.bitnile.kalimandb.storage.RocksStore;
import org.bitnile.kalimandb.storage.Store;
import org.bitnile.kalimandb.storage.option.StoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * @author hecenjie
 */
public class RaftStore extends LifecycleBase {

    private static final Logger logger = LoggerFactory.getLogger(RaftStore.class);

    private final Node node;

    private final Store store;

    public RaftStore(Node node, StoreConfig storeConfig){
        this.node = node;
        this.store = new RocksStore(storeConfig);
    }

    public RaftStore(Node node, Store store) {
        this.node = node;
        this.store = store;
    }

    /**
     * PUT operation will be applied to {@link StoreStateMachine} for execution. Note that do not need to wait
     * until the operation is applied to the state machine to reply to the client. It can reply to the client
     * just when the logs are copied to most of the cluster and then through {@link StoreClosure#onCommitted()}.
     *
     * @param key key for put
     * @param value value for put
     * @param closure will be called for response to the client
     */
    public void put(byte[] key, byte[] value, UUID commandId, StoreClosure closure) {
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.PUT, key, value, commandId);
        closure.setStoreOperation(storeOperation);
        applyOperation(storeOperation, closure);
    }

    /**
     * The GET operation first attempts to use readIndex to guarantee linear consistent reading. If this method succeeds,
     * it can directly respond to the client. Otherwise, if the current node is the leader, try to apply the read operation
     * like {@link #put(byte[], byte[], UUID, StoreClosure)} or {@link #delete(byte[], UUID, StoreClosure)}.
     *
     * @param key key for get
     * @param closure will be called for response to the client
     */
    public void get(byte[] key, UUID commandId, StoreClosure closure) {
        byte[] reqContext = new byte[0];
        this.node.readIndex(reqContext, new ReadIndexClosure() {
            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                // See https://pingcap.com/blog-cn/lease-read/#readindex-read
                if (status.isOk()) {
                    try {
                        byte[] val = RaftStore.this.store.get(key);
                        success(closure, val);
                    } catch (StoreException ex){
                        logger.error("Failed to GET, error on RocksStore", ex);
                        failure(closure, "Failed to GET, error on RocksStore");
                    }
                    return;
                }
                if (isLeader()) {
                    // If 'read index' read fails, try to applying to the state machine at the leader node
                    logger.warn("Failed to GET with 'ReadIndex': {}, try to applying to the state machine.", status);
                    StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.GET, key, commandId);
                    closure.setStoreOperation(storeOperation);
                    applyOperation(storeOperation, closure);
                } else {
                    logger.error("Failed to GET with 'ReadIndex': {}.", status);
                    failure(closure, "Failed to GET with 'ReadIndex'");
                }
            }
        });
    }

    /**
     * The GET_START_WITH operation first attempts to use readIndex to guarantee linear consistent reading. If this method succeeds,
     * it can directly respond to the client. Otherwise, if the current node is the leader, try to apply the read operation
     * like {@link #put(byte[], byte[], UUID, StoreClosure)} or {@link #delete(byte[], UUID, StoreClosure)}.
     *
     * @param prefix the prefix of key for get start with
     * @param closure will be called for response to the client
     */
    public void getStartWith(byte[] prefix, UUID commandId, StoreClosure closure) {
        byte[] reqContext = new byte[0];
        this.node.readIndex(reqContext, new ReadIndexClosure() {
            @Override
            public void run(final Status status, final long index, final byte[] reqCtx) {
                // See https://pingcap.com/blog-cn/lease-read/#readindex-read
                if (status.isOk()) {
                    try {
                        Map<byte[], byte[]> val = RaftStore.this.store.getStartWith(prefix);
                        success(closure, val);
                    } catch (StoreException ex){
                        logger.error("Failed to GET_START_WITH, error on RocksStore", ex);
                        failure(closure, "Failed to GET_START_WITH, error on RocksStore");
                    }
                    return;
                }
                if (isLeader()) {
                    // If 'read index' read fails, try to applying to the state machine at the leader node
                    logger.warn("Failed to GET_START_WITH with 'ReadIndex': {}, try to applying to the state machine.", status);
                    StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.GET_START_WITH, prefix, commandId);
                    closure.setStoreOperation(storeOperation);
                    applyOperation(storeOperation, closure);
                } else {
                    logger.error("Failed to GET_START_WITH with 'ReadIndex': {}.", status);
                    failure(closure, "Failed to GET_START_WITH with 'ReadIndex'");
                }
            }
        });
    }

    /**
     * DELETE operation will be applied to {@link StoreStateMachine} for execution. Note that do not need to wait
     * until the operation is applied to the state machine to reply to the client. It can reply to the client
     * just when the logs are copied to most of the cluster and then through {@link StoreClosure#onCommitted()}.
     *
     * @param key key for delete
     * @param closure will be called for response to the client
     */
    public void delete(byte[] key, UUID commandId, StoreClosure closure){
        StoreOperation storeOperation = StoreOperation.newOperation(StoreOperation.DELETE, key, commandId);
        closure.setStoreOperation(storeOperation);
        applyOperation(storeOperation, closure);
    }

    /**
     * Apply the operation. This operation will be replicated to majority nodes in the cluster in the form of log,
     * and then can be applied by {@link StoreStateMachine#onApply(Iterator)}.
     *
     * @param storeOperation the operation (including key and value)
     * @param closure will be called for response to the client
     */
    private void applyOperation(final StoreOperation storeOperation, StoreClosure closure) {
        if (!isLeader()) {
            closure.setError(ResponseCode.NOT_LEADER);
            closure.run(new Status(RaftError.ENEWLEADER, "Not leader"));
            return;
        }
        final Task task = new Task();
        try {
            task.setData(ByteBuffer.wrap(SerializerFactory.get(SerializerType.MESSAGE_PACK).write(storeOperation)));
            task.setDone(closure);
        } catch(Exception ex){
            logger.error("Failed to serialize the storeOperation", ex);
            closure.setError(ResponseCode.INVALID_PARAMETER);
            closure.run(new Status(RaftError.UNKNOWN, "Failed to serialize the storeOperation"));
        }
        this.node.apply(task);
    }

    @Override
    protected void initInternal() {
        store.init();
    }

    @Override
    protected void startInternal() {
        /// moved to readSnapshot()
        // delete the data first for raft
//        if(store instanceof RocksStore){
//            ((RocksStore)store).clean();
//        }
        store.start();
    }

    @Override
    protected void stopInternal() {
        store.stop();
    }

    @Override
    protected void destroyInternal() {
        store.destroy();
    }

    private boolean isLeader() {
        return this.node.isLeader();
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

    public Node getNode() {
        return node;
    }

    public Store getStore() {
        return store;
    }
}
