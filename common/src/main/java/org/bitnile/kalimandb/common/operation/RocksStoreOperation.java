package org.bitnile.kalimandb.common.operation;

/**
 * Represent the operations of RocksStore class.
 *
 * @author hecenjie
 */
public class RocksStoreOperation extends StoreOperation {

    public static final String DESTROY_ROCKS_DB = "DESTROY_ROCKS_DB";

    public static final String OPEN_ROCKS_DB = "OPEN_ROCKS_DB";

    public static final String WRITE_SNAPSHOT = "WRITE_SNAPSHOT";

    public static final String LOAD_SNAPSHOT = "LOAD_SNAPSHOT";

    public static final String CLEAR_ROCKS_DB = "CLEAR_ROCKS_DB";

}
