package org.bitnile.kalimandb.common;

/**
 * It's actually a state machine that manage the lifecycle of an object.
 */
public interface Lifecycle {

    void init();

    void start();

    void stop();

    void destroy();

    LifecycleState getState();

    String getStateName();

}
