package org.bitnile.kalimandb.client.kaliman;

import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.document.Document;
import org.bitnile.kalimandb.common.exception.DatabaseClientException;


public interface KalimanDB {
    Result insert(Document document) throws DatabaseClientException;

    Result update(Document document) throws DatabaseClientException;

    Result delete(String id) throws DatabaseClientException;

    CompositeResult find(String id) throws DatabaseClientException;

    CompositeResult findStartWith(String prefix) throws DatabaseClientException;
}
