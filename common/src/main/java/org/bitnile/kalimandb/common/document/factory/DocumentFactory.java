package org.bitnile.kalimandb.common.document.factory;

import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.document.impl.DocumentType;

public interface DocumentFactory {

    Document getByType(DocumentType documentType);

    default Document getByType(int typeId) {
        return getByType(DocumentType.getById(typeId));
    }
}
