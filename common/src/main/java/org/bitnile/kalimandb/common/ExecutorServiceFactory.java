package org.bitnile.kalimandb.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.isBlank;


/**
 * Factory of thread pool.
 *
 * @author hecenjie
 */
public class ExecutorServiceFactory {

    private static final String DEFAULT_NAME = "kaliman";

    public static ExecutorService newThreadPool() {
        return newThreadPool(DEFAULT_NAME);
    }

    public static ExecutorService newThreadPool(String name) {
        int core = Runtime.getRuntime().availableProcessors();
        return newThreadPool(core, core << 1, name);
    }

    public static ExecutorService newThreadPool(final int corePoolSize, final int maxPoolSize, String name) {
        if (isBlank(name)) name = DEFAULT_NAME;
        RejectedExecutionHandler wrappedHandler = new RejectedExecutionHandlerWithLog(
                name, new ThreadPoolExecutor.CallerRunsPolicy());
        return newThreadPool(corePoolSize, maxPoolSize, name, new SynchronousQueue<>(), wrappedHandler);
    }

    public static ExecutorService newThreadPool(final int corePoolSize, final int maxPoolSize, String name,
                                                final BlockingQueue<Runnable> workQueue) {
        if (isBlank(name)) name = DEFAULT_NAME;
        RejectedExecutionHandler wrappedHandler = new RejectedExecutionHandlerWithLog(
                name, new ThreadPoolExecutor.CallerRunsPolicy());
        return newThreadPool(corePoolSize, maxPoolSize, name, workQueue, wrappedHandler);
    }

    public static ExecutorService newThreadPool(final int corePoolSize, final int maxPoolSize, String name,
                                                final RejectedExecutionHandler handler) {
        requireNonNull(handler, "Rejected execution handler is null");
        if (isBlank(name)) name = DEFAULT_NAME;
        RejectedExecutionHandler wrappedHandler = new RejectedExecutionHandlerWithLog(name, handler);
        return newThreadPool(corePoolSize, maxPoolSize, name, new SynchronousQueue<>(), handler);
    }

    public static ExecutorService newThreadPool(final int corePoolSize, final int maxPoolSize, String name,
                                                final BlockingQueue<Runnable> workQueue,
                                                final RejectedExecutionHandler handler) {
        requireNonNull(handler, "Rejected execution handler is null");
        if (isBlank(name)) name = DEFAULT_NAME;
        RejectedExecutionHandler wrappedHandler = (handler instanceof RejectedExecutionHandlerWithLog) ?  // avoid logging twice
                handler : new RejectedExecutionHandlerWithLog(name, handler);
        return new ThreadPoolExecutor(corePoolSize, maxPoolSize,
                60L, TimeUnit.SECONDS, workQueue, new NamedThreadFactory(name), wrappedHandler);

    }

    /**
     * A decorator class for traditional policies.
     *
     * @see ThreadPoolExecutor.CallerRunsPolicy
     * @see ThreadPoolExecutor.AbortPolicy
     * @see ThreadPoolExecutor.DiscardPolicy
     * @see ThreadPoolExecutor.DiscardOldestPolicy
     */
    private static class RejectedExecutionHandlerWithLog implements RejectedExecutionHandler {

        private static final Logger logger = LoggerFactory.getLogger(RejectedExecutionHandlerWithLog.class);

        private final String name;
        private final RejectedExecutionHandler rejectedExecutionHandler;

        public RejectedExecutionHandlerWithLog(String name, RejectedExecutionHandler rejectedExecutionHandler) {
            this.name = name;
            this.rejectedExecutionHandler = rejectedExecutionHandler;
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warn("Thread pool with name [{}] capacity exhausted", this.name);
            this.rejectedExecutionHandler.rejectedExecution(r, executor);
        }
    }


}
