package org.bitnile.kalimandb.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;


public class NamedThreadFactory implements ThreadFactory {

    private static final Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);

    private static final Thread.UncaughtExceptionHandler LOG_UNCAUGHT_EX_HANDLER = new LogUncaughtExceptionHandler();
    private static final String SEPARATOR = "-";

    private final AtomicInteger id  = new AtomicInteger();
    private final String prefix;
    private final boolean daemon;

    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        requireNonNull(prefix, "The prefix of thread is null");
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(final Runnable r) {
        if(r == null) throw new IllegalArgumentException("Runnable object must not be null");
        String name = this.prefix + SEPARATOR + id.getAndIncrement();
        Thread t = new Thread(r, name);
        t.setDaemon(this.daemon);
        t.setUncaughtExceptionHandler(LOG_UNCAUGHT_EX_HANDLER);
        logger.info("Created a new thread {}", name);
        return t;
    }

    private static final class LogUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception in thread {}", t, e);
        }
    }

}
