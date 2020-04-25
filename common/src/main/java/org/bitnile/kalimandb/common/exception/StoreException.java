package org.bitnile.kalimandb.common.exception;

/**
 * Represent the exception of Store interface.
 *
 * @author hecenjie
 */
public class StoreException extends RuntimeException{

    public StoreException(String opt) {
        super("Exception during " + opt + " operation");
    }

    public StoreException(String opt, byte[] key) {
        super("Exception during " + opt + " operation with key: " + decode(key));
    }

    public StoreException(String opt, byte[] key, byte[] value) {
        super("Exception during " + opt + " operation with key: " + decode(key) + " and value: " + decode(value));
    }

    public StoreException(String opt, Throwable cause) {
        super("Exception during " + opt + " operation", cause);
    }

    public StoreException(String opt, byte[] key, Throwable cause) {
        super("Exception during " + opt + " operation with key: " + decode(key), cause);
    }

    public StoreException(String opt, byte[] key, byte[] value, Throwable cause) {
        super("Exception during " + opt + " operation with key: " + decode(key) + " and value: " + decode(value), cause);
    }

    // TODO: Need a real decode method, this is so naive
    private static String decode(byte[] bytes){
        if(bytes == null) return "[]";
        String str = new String(bytes);
        return "[" + str + "]";
    }
}
