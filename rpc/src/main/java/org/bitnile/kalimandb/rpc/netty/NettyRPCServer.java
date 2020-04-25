package org.bitnile.kalimandb.rpc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.bitnile.kalimandb.rpc.RPCServer;
import org.bitnile.kalimandb.common.ExecutorServiceFactory;
import org.bitnile.kalimandb.common.NamedThreadFactory;
import org.bitnile.kalimandb.rpc.common.Pair;
import org.bitnile.kalimandb.rpc.exception.RemotingSendRequestException;
import org.bitnile.kalimandb.rpc.exception.RemotingTimeoutException;
import org.bitnile.kalimandb.rpc.exception.RemotingTooMuchRequestException;
import org.bitnile.kalimandb.rpc.protocol.RPCPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;

/**
 * NettyRPCServer is a Netty based bootstrap class for server startup, It is based on {@link RPCPacket} for transmission
 * Relevant configuration needs to be performed through {@link NettyServerConfig}, the processor is registered by
 * requestProcessor , and finally started by {@link #start()}
 *
 * e.g:
 * <p>
 * <pre>
 * public class Server {
 *     public static void main(String[] args) {
 *         NettyServerConfig config = new NettyServerConfig(the port listening on);
 *         NettyRPCServer server = new NettyRPCServer(config);
 *         server.registerProcessor(XXX, new NettyRequestProcessor() {
 *             \@Override
 *             public RPCPacket processRequest(ChannelHandlerContext ctx, RPCPacket request) throws Exception {
 *                 (What to do when XXXcode is received and send the response packet)
 *             }
 *
 *             \@Override
 *             public boolean rejectRequest() {
 *                 return false;
 *             }
 *         },∆ Executors.newSingleThreadExecutor());
 *
 *         server.start();
 *     }
 * }
 * </pre>
 */
public class NettyRPCServer extends AbstractNettyRPC implements RPCServer {
    private static final Logger log = LoggerFactory.getLogger(NettyRPCServer.class);

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup workerEventLoopGroup;
    private final NettyServerConfig nettyServerConfig;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private int port = 0;

    private final ExecutorService publicExecutor;

    private final Timer timer = new Timer("ServerHouseKeepingService", true);


    public NettyRPCServer(NettyServerConfig config) {
        super(config.serverOnewaySemaphoreValue(), config.serverAsyncSemaphoreValue());

        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = config;

        int publicThreadNums = nettyServerConfig.serverCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = ExecutorServiceFactory.newThreadPool(publicThreadNums, publicThreadNums, "NettyServerPublicExecutor");

        this.bossEventLoopGroup = new NioEventLoopGroup(1,
                new NamedThreadFactory("NettyNIOBoss"));
        this.workerEventLoopGroup = new NioEventLoopGroup(config.serverWorkerThreads(),
                new NamedThreadFactory("NettyNIOWorker"));
    }


    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorService = executor;
        if (executorService == null) {
            executorService = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorService);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public RPCPacket invokeSync(Channel channel, RPCPacket request, long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeOneway(Channel channel, RPCPacket request, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingTooMuchRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.serverWorkerThreads(),
                new NamedThreadFactory("NettyServerCodecThread"));

        NettyServerHandler serverHandler = new NettyServerHandler();

        serverBootstrap.group(this.bossEventLoopGroup, this.workerEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.serverSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.serverSocketRcvBufSize())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch){
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(defaultEventExecutorGroup, new Encoder(), new Decoder(), serverHandler);
                    }
                });

        if (nettyServerConfig.serverPooledByteBufAllocatorEnable()) {
            serverBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture future = serverBootstrap.bind(this.nettyServerConfig.port()).sync();
            InetSocketAddress addr = (InetSocketAddress) future.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e) {
            throw new RuntimeException("Netty server bind synchronously InterruptedException", e);
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRPCServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            this.bossEventLoopGroup.shutdownGracefully();

            this.workerEventLoopGroup.shutdownGracefully();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRPCServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRPCServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RPCPacket> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RPCPacket msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }
}
