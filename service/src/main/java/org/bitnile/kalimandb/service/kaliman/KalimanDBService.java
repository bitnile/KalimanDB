package org.bitnile.kalimandb.service.kaliman;

import org.bitnile.kalimandb.service.status.CompositeResult;
import org.bitnile.kalimandb.service.status.Result;
import org.bitnile.kalimandb.common.Lifecycle;
import org.bitnile.kalimandb.common.document.Document;

import java.util.UUID;

public interface KalimanDBService extends Lifecycle {
    Result insert(UUID commandId, Document document);

    Result update(UUID commandId, Document document);

    Result delete(UUID commandId, Object id);

    CompositeResult find(UUID commandId, Object id);

    CompositeResult findStartWith(UUID commandId, Object prefix);
}
