package org.bitnile.kalimandb.service.task;

import org.bitnile.kalimandb.common.ExecutorServiceFactory;
import org.bitnile.kalimandb.common.LifecycleBase;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class RetryTaskManager extends LifecycleBase {
    private Queue<DBOperationTask> queue;
    private ExecutorService executor;
    private int maxRetryTime;

    public RetryTaskManager() {
        queue = new LinkedBlockingDeque<>();
        executor = ExecutorServiceFactory.newThreadPool(1, 4, "RetryTaskManagerThread");
    }

    public void submit(DBOperationTask task) {
//        this.executor.submit(task);
    }

    @Override
    public void destroyInternal() {
        this.executor.shutdown();
    }

    static class WrapperTask {
        private DBOperationTask task;
        private int time;
        private int maxRetryTime;

        WrapperTask(DBOperationTask task, int maxRetryTime) {
            this.task = task;
            this.maxRetryTime = maxRetryTime;
            this.time = 0;
        }

        boolean shouldEnd() {
            return time >= maxRetryTime;
        }

//        Future run() {
//            return task.
//        }
    }
}
