package org.bitnile.kalimandb.rpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.bitnile.kalimandb.rpc.InvokeCallback;
import org.bitnile.kalimandb.rpc.common.RemotingUtil;
import org.bitnile.kalimandb.rpc.common.Pair;
import org.bitnile.kalimandb.rpc.common.SemaphoreReleaseOnlyOnce;
import org.bitnile.kalimandb.common.protocol.ResponseCode;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

public abstract class AbstractNettyRPC {
    private static final Logger log = LoggerFactory.getLogger(AbstractNettyRPC.class);

    protected final Semaphore semaphoreOneway;
    protected final Semaphore semaphoreAsync;
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);
    protected final ConcurrentMap<Integer /* requestId */, ResponseFuture> responseTable =
            new ConcurrentHashMap<Integer, ResponseFuture>(256);
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;


    public AbstractNettyRPC(int onewayNum, int asyncNum) {
        this.semaphoreOneway = new Semaphore(onewayNum);
        this.semaphoreAsync = new Semaphore(asyncNum);
    }

    public void processMessageReceived(ChannelHandlerContext ctx, RPCPacket msg) throws Exception {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST_PACKET:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_PACKET:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RPCPacket pck) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(pck.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        final int requestId = pck.getRequestId();

        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        final RPCPacket response = pair.getObject1().processRequest(ctx, pck);

                        if (!pck.isOnewayRPC()) {
                            if (response != null) {
                                response.setRequestId(requestId);
                                response.markResponseType();
                                try {
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    log.error("process request over, but response failed", e);
                                    log.error(pck.toString());
                                    log.error(response.toString());
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("process the request error!", e);
                        log.error(pck.toString());

                        if (!pck.isOnewayRPC()) {
                            final RPCPacket response = RPCPacket.createResponsePacket(ResponseCode.SYSTEM_ERROR,
                                    "send response fail, Exception:" + e.toString());
                            response.setRequestId(requestId);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            if (pair.getObject1().rejectRequest()) {
                final RPCPacket response = RPCPacket.createResponsePacket(ResponseCode.SERVER_BUSY,
                        "system is busy, try again for a while");
                response.setRequestId(requestId);
                ctx.writeAndFlush(response);
            }

            try {
                RequestTask task = new RequestTask(run, ctx.channel(), pck);
                pair.getObject2().submit(task);
            } catch (RejectedExecutionException e){
                log.warn("The ExecutorService: " + pair.getObject2().toString() + " is too busy");
                if (!pck.isOnewayRPC()) {
                    final RPCPacket response = RPCPacket.createResponsePacket(ResponseCode.SERVER_BUSY,
                            "system is busy, try again for a while");
                    response.setRequestId(requestId);
                    ctx.writeAndFlush(response);
                }
            }

        } else {
            String error = "request type " + pck.getCode() + " not supported";
            final RPCPacket response = RPCPacket.createResponsePacket(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setRequestId(requestId);
            ctx.writeAndFlush(response);
        }
    }

    public void processResponseCommand(ChannelHandlerContext ctx, RPCPacket pck) {
        final int request = pck.getRequestId();
        final ResponseFuture responseFuture = responseTable.get(request);

        if (responseFuture != null) {
            responseFuture.setRPCPacket(pck);

            responseTable.remove(request);

            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(pck);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, addr: {}", RemotingUtil.parseChannelRemoteAddr(ctx.channel()));
            log.warn(pck.toString());
        }

    }

    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();

        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                log.warn("execute the callback in exception :", e);
                runInThisThread = true;
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }

    }

    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    public abstract ExecutorService getCallbackExecutor();

    public RPCPacket invokeSyncImpl(final Channel channel, final RPCPacket packet, final long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException {
        final int requestId = packet.getRequestId();

        try {
            ResponseFuture responseFuture = new ResponseFuture(channel, requestId, timeoutMillis, null, null);
            final SocketAddress addr = channel.remoteAddress();
            responseTable.put(requestId, responseFuture);
            channel.writeAndFlush(packet).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(requestId);
                    responseFuture.setCause(future.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            RPCPacket response = responseFuture.waitResponse(timeoutMillis);

            if (response == null) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingUtil.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingUtil.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }

            return response;
        } finally {
            responseTable.remove(requestId);
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RPCPacket packet, final long timeoutMillis,
                                final InvokeCallback callback)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingTooMuchRequestException {
        long beginStartTime = System.currentTimeMillis();
        final int requestId = packet.getRequestId();

        boolean acquired = semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);

        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            final ResponseFuture responseFuture = new ResponseFuture(channel, requestId, timeoutMillis, callback, once);
            this.responseTable.put(requestId, responseFuture);

            try {
                channel.writeAndFlush(packet).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        requestFail(requestId);
                        log.warn("send a request command to channel <{}> failed.", RemotingUtil.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingUtil.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingUtil.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis,
                                this.semaphoreAsync.getQueueLength(),
                                this.semaphoreAsync.availablePermits()
                        );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }


    public void invokeOnewayImpl(final Channel channel, final RPCPacket packet, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingTooMuchRequestException {
        packet.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(packet).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingUtil.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreOneway.getQueueLength(),
                        this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

}
