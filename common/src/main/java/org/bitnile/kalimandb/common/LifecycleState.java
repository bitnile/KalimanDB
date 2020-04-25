package org.bitnile.kalimandb.common;

public enum LifecycleState {
    NEW(false),
    INITIALIZING(false),
    INITIALIZED(false),
    STARTING(true),
    STARTED(true),
    STOPPING(false),
    STOPPED(false),
    DESTROYING(false),
    DESTROYED(false),
    FAILED(false);

    private final boolean available;

    LifecycleState(boolean available) {
        this.available = available;
    }

    public boolean isAvailable() {
        return available;
    }
}
