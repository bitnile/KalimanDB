package org.bitnile.kalimandb.common.serializer;

import org.bitnile.kalimandb.common.exception.SerializeException;

import java.io.UnsupportedEncodingException;

public class StringSerializer implements Serializer {
    @Override
    public byte[] write(Object obj){
        if(!(obj instanceof String)) {
            throw new IllegalArgumentException("Parameter should be String class");
        }
        String s = (String)obj;
        return s.getBytes();
    }

    @Override
    public <T> T read(byte[] bytes, Class<T> tClass){
        if(!(tClass == String.class)){
            throw new IllegalArgumentException("Parameter should be String class");
        }
        try {
            return (T) new String(bytes, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
    }
}
