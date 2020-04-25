package org.bitnile.kalimandb.common.serializer;


import org.bitnile.kalimandb.common.exception.SerializeException;

public interface Serializer {

    byte[] write(Object obj) throws SerializeException;

    <T> T read(byte[] bytes, Class<T> tClass) throws SerializeException;
}
