package org.bitnile.kalimandb.common.document;

import java.util.Map;

public interface Document {

    Document append(String field, Object value);

    Object valueOf(String field);

    Object id();

    DocumentType type();

    Map<String, Object> getContent();
}
