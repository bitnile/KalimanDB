package org.bitnile.kalimandb.storage;

import org.bitnile.kalimandb.common.Lifecycle;
import org.bitnile.kalimandb.common.StoreClosure;
import org.bitnile.kalimandb.common.exception.StoreException;

import java.util.List;
import java.util.Map;

/**
 * Provide KV storage API
 */
public interface Store extends Lifecycle {

    void put(byte[] key, byte[] value) throws StoreException;

    byte[] get(byte[] key) throws StoreException;

    Map<byte[], byte[]> getStartWith(byte[] prefix) throws StoreException;

    void delete(byte[] key) throws StoreException;

}
