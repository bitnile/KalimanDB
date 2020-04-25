package org.bitnile.kalimandb.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class NamedThreadFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNewThread() throws InterruptedException {
        int n = 10;
        NamedThreadFactory namedThreadFactory = new NamedThreadFactory("test");
        Thread[] threads = new Thread[n];
        AtomicInteger num = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(n);
        for(int i = 0; i < n; i++){
            threads[i] = namedThreadFactory.newThread(()->{
                num.getAndIncrement();
                latch.countDown();
            });
        }
        for(int i = 0; i < n; i++){
            assertEquals(threads[i].getName(), "test-" + i);
            threads[i].start();
        }
        latch.await();
        assertEquals(num.get(), n);
    }
}