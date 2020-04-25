package org.bitnile.kalimandb.common.serializer;


public class SerializerFactory {
    private static final CommonSerializerContainer<Serializer> container = new CommonSerializerContainer<>();

    static {
        put(SerializerType.JSON, new JSONSerializer());
        put(SerializerType.MESSAGE_PACK, new MsgPackSerializer());
        put(SerializerType.STRING, new StringSerializer());
    }

    public static Serializer get(SerializerType type) {
        return container.getSerializer(type);
    }

    public static void put(SerializerType type, Serializer serializer) {
        container.putSerializer(type, serializer);
    }

}
