package org.bitnile.kalimandb.common;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.error.RaftError;
import org.bitnile.kalimandb.common.operation.StoreOperation;
import org.bitnile.kalimandb.common.protocol.ResponseCode;

import java.util.concurrent.CountDownLatch;

public class StoreClosure implements TaskClosure {

    private volatile int error;
    private volatile Object data;
    private volatile StoreOperation storeOperation;
    private volatile CountDownLatch latch;

    public int getError() {
        return error;
    }

    public void setError(int error) {
        this.error = error;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public StoreOperation getStoreOperation() {
        return storeOperation;
    }

    public void setStoreOperation(StoreOperation storeOperation) {
        this.storeOperation = storeOperation;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public boolean isSuccess() {
        return getError() == 0;
    }

    @Override
    public void run(Status status) {
//        if (!status.isOk()) {
//            error = ResponseCode.RAFT_TIMEOUT;
//        }
        latch.countDown();
    }

    /**
     * Called when task is committed to majority peers of the
     * RAFT group but before it is applied to state machine.
     *
     * <strong>Note: user implementation should not block
     * this method and throw any exceptions.</strong>
     */
    @Override
    public void onCommitted() {
        /// Do not use this method temporarily
//        if(storeOperation != null){
//            if(StoreOperation.PUT.equals(storeOperation.getOp()) || StoreOperation.DELETE.equals(storeOperation.getOp())){
//                latch.countDown();
//            }
//        }
    }
}
