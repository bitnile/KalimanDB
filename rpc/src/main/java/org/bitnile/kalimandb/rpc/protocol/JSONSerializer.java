package org.bitnile.kalimandb.rpc.protocol;


import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;

public class JSONSerializer {
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    public static byte[] encode(Object obj) {
        final String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }


    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

}
