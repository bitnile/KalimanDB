package org.bitnile.kalimandb.service.option;

public class ServiceConfig {
    private int maxWaitTime = 2;
    private int maxRetryTime = 3;

    public int maxWaitTime() {
        return maxWaitTime;
    }

    public ServiceConfig maxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
        return this;
    }

    public int maxRetryTime() {
        return maxRetryTime;
    }

    public ServiceConfig maxRetryTime(int maxRetryTime) {
        this.maxRetryTime = maxRetryTime;
        return this;
    }
}
