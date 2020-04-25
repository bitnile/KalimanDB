package org.bitnile.kalimandb.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class LifecycleBaseTest {

    private LifecycleBean target;

    @Before
    public void setUp() throws Exception {
        target = new LifecycleBean();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testRoutinely(){
        target.init();
        assertEquals(LifecycleState.INITIALIZED, target.getState());
        target.start();
        assertEquals(LifecycleState.STARTED, target.getState());
        target.stop();
        assertEquals(LifecycleState.STOPPED, target.getState());
        target.destroy();
        assertEquals(LifecycleState.DESTROYED, target.getState());
    }

    @Test
    public void testStartDirectly(){
        target.start();
        assertEquals(LifecycleState.STARTED, target.getState());
    }

    @Test
    public void testStopAfterInit(){
        target.init();
        target.stop();
        assertEquals(LifecycleState.STOPPED, target.getState());
    }

    @Test
    public void testDestroyAfterInit(){
        target.init();
        target.destroy();
        assertEquals(LifecycleState.DESTROYED, target.getState());
    }

    @Test
    public void testStartAfterStop(){
        target.init();
        target.start();
        target.stop();
        assertEquals(LifecycleState.STOPPED, target.getState());
        target.start();
        assertEquals(LifecycleState.STARTED, target.getState());
    }

    @Test
    public void testStopWhenFailed(){
        target.setCtl(LifecycleBean.BAD_INIT);
        try {
            target.init();
        } catch(RuntimeException ex){
            assertEquals(LifecycleState.FAILED, target.getState());
            target.stop();
            assertEquals(LifecycleState.STOPPED, target.getState());
        }
    }

    @Test
    public void testDestroyWhenFailed(){
        target.setCtl(LifecycleBean.BAD_INIT);
        try {
            target.init();
        } catch(RuntimeException ex){
            assertEquals(LifecycleState.FAILED, target.getState());
            target.destroy();
            assertEquals(LifecycleState.DESTROYED, target.getState());
        }
    }
}

class LifecycleBean extends LifecycleBase{

    public static final int BAD_INIT = 1;
    public static final int BAD_START = 1 << 1;
    public static final int BAD_STOP = 1 << 2;
    public static final int BAD_DESTROY = 1 << 3;

    private int ctl;

    public LifecycleBean(){
    }

    public LifecycleBean(byte ctl) {
        this.ctl = ctl;
    }

    @Override
    protected void initInternal() {
        if((ctl & BAD_INIT) != 0) throw new RuntimeException("Fail to init");
    }

    @Override
    protected void startInternal() {
        if((ctl & BAD_START) != 0) throw new RuntimeException("Fail to start");
    }

    @Override
    protected void stopInternal() {
        if((ctl & BAD_STOP) != 0) throw new RuntimeException("Fail to stop");
    }

    @Override
    protected void destroyInternal() {
        if((ctl & BAD_DESTROY) != 0) throw new RuntimeException("Fail to destroy");
    }

    public void setCtl(int ctl) {
        this.ctl = ctl;
    }

    public int getCtl() {
        return ctl;
    }
}

