package org.bitnile.kalimandb.common.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bitnile.kalimandb.common.exception.SerializeException;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;


public class MsgPackSerializer implements Serializer {
    private static final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    @Override
    public byte[] write(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public <T> T read(byte[] bytes, Class<T> tClass){
        try {
            return mapper.readValue(bytes, tClass);
        } catch (IOException e) {
            throw new SerializeException(e);
        }
    }
}
