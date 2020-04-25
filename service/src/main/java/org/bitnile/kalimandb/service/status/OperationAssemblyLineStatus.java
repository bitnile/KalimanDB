package org.bitnile.kalimandb.service.status;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class OperationAssemblyLineStatus {
    private volatile AtomicInteger count;
    private volatile AtomicBoolean stop;
    private CountDownLatch latch;

    public OperationAssemblyLineStatus(int count, CountDownLatch latch) {
        this.count = new AtomicInteger(count);
        this.stop = new AtomicBoolean(false);
        this.latch = latch;
    }

    public boolean isSuccess() {
        return count.get() == 0;
    }

    public void done() {
        count.decrementAndGet();
        latch.countDown();
    }

    public boolean isStop() {
        return stop.get();
    }

    public void setStop(boolean stop) {
        this.stop.set(stop);
    }
}
