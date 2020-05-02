package org.bitnile.kalimandb.storage;

import org.bitnile.kalimandb.common.Lifecycle;
import org.bitnile.kalimandb.common.exception.StoreException;
import java.util.Map;

/**
 * Provide KV storage API
 */
public interface Store extends Lifecycle {

    void put(byte[] key, byte[] value) throws StoreException;

    byte[] get(byte[] key) throws StoreException;

    Map<byte[], byte[]> getStartWith(byte[] prefix) throws StoreException;

    void delete(byte[] key) throws StoreException;

    void writeSnapshot(String snapshotPath) throws StoreException;

    void readSnapshot(String snapshotPath) throws StoreException;

}
