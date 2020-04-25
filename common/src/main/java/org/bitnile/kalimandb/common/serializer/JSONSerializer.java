package org.bitnile.kalimandb.common.serializer;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;


public class JSONSerializer implements Serializer{
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    @Override
    public byte[] write(Object obj) {
        final String json = toJson(obj, false);
        return json.getBytes(CHARSET_UTF8);
    }

    @Override
    public <T> T read(byte[] bytes, Class<T> tClass) {
        final String json = new String(bytes, CHARSET_UTF8);
        return fromJson(json, tClass);
    }


    private String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    private <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }



}
