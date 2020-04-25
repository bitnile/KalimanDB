package org.bitnile.kalimandb.service.task;

import org.bitnile.kalimandb.service.status.DBOperationStatus;

public class DBOperationResult {
    private DBOperationStatus status;

    public DBOperationStatus getStatus() {
        return status;
    }

    public void setStatus(DBOperationStatus status) {
        this.status = status;
    }
}
