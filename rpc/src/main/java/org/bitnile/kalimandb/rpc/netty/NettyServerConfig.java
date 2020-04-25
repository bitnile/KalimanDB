package org.bitnile.kalimandb.rpc.netty;

public class NettyServerConfig {
    /** Listening port */
    private int port = 8991;
    /** Number of threads processing IO */
    private int serverWorkerThreads = 3;
    /** Number of Callback Executor Threads */
    private int serverCallbackExecutorThreads = 0;
    /** Permit of Semaphore for client sending one-way packet */
    private int serverOnewaySemaphoreValue = 256;
    /** Permit of Semaphore for client sending packet asynchronously */
    private int serverAsyncSemaphoreValue = 64;

    /** Send buffer size */
    private int serverSocketSndBufSize = 65535;
    /** Receive buffer size */
    private int serverSocketRcvBufSize = 65535;
    /** Use ByteBuf pool or not */
    private boolean serverPooledByteBufAllocatorEnable = true;

    public NettyServerConfig() {
    }

    public NettyServerConfig(int port) {
        this.port = port;
    }

    public int port() {
        return port;
    }

    public NettyServerConfig port(int port) {
        this.port = port;
        return this;
    }


    public int serverOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public NettyServerConfig serverOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
        return this;
    }

    public int serverAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public NettyServerConfig serverAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
        return this;
    }

    public int serverSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public NettyServerConfig serverSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
        return this;
    }

    public int serverSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public NettyServerConfig serverSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
        return this;
    }

    public boolean serverPooledByteBufAllocatorEnable() {
        return serverPooledByteBufAllocatorEnable;
    }

    public NettyServerConfig serverPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
        this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
        return this;
    }

    public int serverWorkerThreads() {
        return serverWorkerThreads;
    }

    public NettyServerConfig serverWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
        return this;
    }

    public int serverCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public NettyServerConfig serverCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
        return this;
    }

}
