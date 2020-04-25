package org.bitnile.kalimandb.common.document.impl;

public enum DocumentType {
    Default(0);

    private int id;

    DocumentType(int id) {
        this.id = id;
    }

    public static DocumentType getById(int id) {
        DocumentType[] types = values();

        for (DocumentType documentType : types) {
            if (documentType.getId() == id) {
                return documentType;
            }
        }

        return null;
    }

    public int getId() {
        return id;
    }
}
