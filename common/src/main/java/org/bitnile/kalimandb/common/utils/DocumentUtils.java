package org.bitnile.kalimandb.common.utils;


import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.impl.DefaultDocument;
import org.bitnile.kalimandb.common.serializer.SerializerFactory;
import org.bitnile.kalimandb.common.serializer.SerializerType;

import java.util.LinkedHashMap;

public class DocumentUtils {
    public static Document linkedHashMap2Document(LinkedHashMap map) {

        Document document = new DefaultDocument(map);

        return document;
    }

    public static byte[][] documents2Bytes(Document[] documents) throws Exception {
        byte[][] res = new byte[documents.length][];
        for (int i = 0; i < documents.length; i++) {
            res[i] = SerializerFactory.get(SerializerType.MESSAGE_PACK).write(documents[i].getContent());
        }
        return res;
    }
}
