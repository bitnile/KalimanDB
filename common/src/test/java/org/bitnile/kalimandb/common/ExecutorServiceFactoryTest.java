package org.bitnile.kalimandb.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

public class ExecutorServiceFactoryTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNewThreadPool() {
        ExecutorService executorService = ExecutorServiceFactory.newThreadPool(0, 2, "");
        executorService.execute(()->{});
        executorService.execute(()->{});
        executorService.execute(()->{});
    }
}