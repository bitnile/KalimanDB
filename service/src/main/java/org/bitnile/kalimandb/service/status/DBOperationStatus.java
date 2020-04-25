package org.bitnile.kalimandb.service.status;

public enum DBOperationStatus {
    SUCCESS(0),
    FAILURE(1);

    private int status;

    DBOperationStatus(int status) {
        this.status = status;
    }

    public static DBOperationStatus valueOf(int code) {
        for (DBOperationStatus dbOperationStatus : DBOperationStatus.values()) {
            if (dbOperationStatus.getStatus() == code) {
                return dbOperationStatus;
            }
        }
        return null;
    }

    public int getStatus() {
        return status;
    }
}
