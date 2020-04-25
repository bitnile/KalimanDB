package org.bitnile.kalimandb.common.document.impl;

import java.util.*;

/**
 * @author ITcathyh
 */
public class DefaultDocument extends AbstractDocument {
    private static final DocumentType type = DocumentType.Default;

    public DefaultDocument(Map<String, Object> initMap) {
        super(initMap);
    }

    public DefaultDocument(){
        super();
    }

    public DefaultDocument(int size){
        super(size);
    }

    @Override
    protected void append0(String field, Object value) {
        content.put(field, value);
    }

    @Override
    protected Object get0(String field) {
        return content.get(field);
    }

    @Override
    public DocumentType type() {
        return type;
    }


    @Override
    public String toString() {
        return "DefaultDocument{" +
                "map=" + content +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultDocument document = (DefaultDocument) o;
        return Objects.equals(content, document.content);
    }


    @Override
    public int hashCode() {
        return Objects.hash(content);
    }
}
