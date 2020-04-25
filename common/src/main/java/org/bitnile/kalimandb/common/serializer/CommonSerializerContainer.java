package org.bitnile.kalimandb.common.serializer;

import java.util.EnumMap;
import java.util.Map;

public class CommonSerializerContainer<T> {
    private final Map<SerializerType, T> serializerMap = new EnumMap<>(SerializerType.class);

    public T getSerializer(SerializerType type) {
        return serializerMap.get(type);
    }

    public void putSerializer(SerializerType type, T serializer) {
        serializerMap.put(type, serializer);
    }
}
