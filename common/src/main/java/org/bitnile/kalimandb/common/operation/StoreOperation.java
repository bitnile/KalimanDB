package org.bitnile.kalimandb.common.operation;

import java.util.Arrays;
import java.util.UUID;

/**
 * Represent the operations of Store interface.
 *
 * @author hecenjie
 */
public class StoreOperation {
    
    public static final String PUT = "PUT";
    
    public static final String GET = "GET";
    
    public static final String GET_START_WITH = "GET_START_WITH";
    
    public static final String DELETE = "DELETE";

    private String op;
    private byte[] key;
    private byte[] val;
    private UUID commandId;

    public static StoreOperation newOperation(String op, byte[] key, byte[] val, UUID commandId){
        StoreOperation storeOperation = new StoreOperation();
        storeOperation.setOp(op).setKey(key).setVal(val).setCommandId(commandId);
        return storeOperation;
    }

    public static StoreOperation newOperation(String op, byte[] key, UUID commandId){
        StoreOperation storeOperation = new StoreOperation();
        storeOperation.setOp(op).setKey(key).setCommandId(commandId);
        return storeOperation;
    }

    public final StoreOperation setKey(byte[] key) {
        this.key = key;
        return this;
    }

    public final StoreOperation setVal(byte[] val) {
        this.val = val;
        return this;
    }

    public final StoreOperation setOp(String op) {
        this.op = op;
        return this;
    }

    public final byte[] getKey() {
        return key;
    }

    public final byte[] getVal() {
        return val;
    }

    public final String getOp() {
        return op;
    }

    public UUID getCommandId() {
        return commandId;
    }

    public void setCommandId(UUID commandId) {
        this.commandId = commandId;
    }

    @Override
    public String toString() {
        return "StoreOperation{" +
                "op='" + op + '\'' +
                ", key=" + Arrays.toString(key) +
                ", val=" + Arrays.toString(val) +
                ", commandId=" + commandId +
                '}';
    }
}
