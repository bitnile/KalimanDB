package org.bitnile.kalimandb.common.document.impl;

import org.apache.commons.lang.StringUtils;
import org.bitnile.kalimandb.common.document.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * @author ITcathyh
 */
public abstract class AbstractDocument implements Document {
    public static final String ID_NAME = "_id";
    public static final String TYPE_NAME = "_type";

    protected Map<String, Object> content;

    protected AbstractDocument() {
        this.content = new LinkedHashMap<>();
        init();
    }

    public AbstractDocument(Map<String, Object> initMap) {
        Objects.requireNonNull(initMap);
        this.content = initMap;
        init();
    }

    public AbstractDocument(int size){
        if(size <= 0){
            throw new IllegalArgumentException();
        }
        this.content = new LinkedHashMap<>(size);
        init();
    }

    public AbstractDocument append(String field, Object value) {
        Objects.requireNonNull(value, "value");
        if(StringUtils.isBlank(field)){
            throw new IllegalArgumentException("field should not be blank");
        }
        if(ID_NAME.equals(field) && !(value instanceof String)){
            throw new IllegalArgumentException("value of _id field should be String");
        }
        append0(field, value);
        return this;
    }

    public Object valueOf(String field){
        if(StringUtils.isBlank(field)){
            throw new IllegalArgumentException("field should not be blank");
        }
        return get0(field);
    }

    public Object id() {
        return valueOf(ID_NAME);
    }

    @Override
    public Map<String, Object> getContent(){
        return content;
    }

    protected void init(){
        append(TYPE_NAME, type().getId());
        if(id() == null){
            append(ID_NAME, UUID.randomUUID().toString());
        }
    }

    public abstract DocumentType type();

    protected abstract void append0(String field, Object value);

    protected abstract Object get0(String field);
}
