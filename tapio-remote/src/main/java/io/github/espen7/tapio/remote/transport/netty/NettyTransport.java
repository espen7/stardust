/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.remote.transport.netty;

import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.remote.transport.AssociationHandle;
import io.github.espen7.tapio.remote.transport.Transport;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 Netty 4.2 TCP 的 Classic Remoting 传输实现。
 */
public class NettyTransport implements Transport {
    private final NettyTransportSettings settings;
    private final ExtendedActorSystem system;
    private final LoggingAdapter log;
    private final EventLoopGroup serverGroup;
    private final EventLoopGroup clientGroup;
    private final ChannelGroup allChannels;
    private volatile Address boundAddress;
    private volatile Address localAddress;
    private volatile CompletableFuture<AssociationEventListener> associationListenerPromise;

    /**
     * 构造 Netty 传输实例（由 {@code DynamicAccess} 反射调用）。
     *
     * @param system 所属 {@link ExtendedActorSystem}
     * @param conf 传输配置节
     */
    public NettyTransport(ExtendedActorSystem system, Config conf) {
        this.system = system;
        this.settings = new NettyTransportSettings(conf);
        this.log = Logging.getLogger(system, this, getClass());
        this.serverGroup = new MultiThreadIoEventLoopGroup(
                settings.ServerSocketWorkerPoolSize, system.threadFactory(), NioIoHandler.newFactory());
        this.clientGroup = new MultiThreadIoEventLoopGroup(
                settings.ClientSocketWorkerPoolSize, system.threadFactory(), NioIoHandler.newFactory());
        this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }

    /**
     * 构造 Netty 传输实例（兼容参数顺序）。
     *
     * @param conf 传输配置节
     * @param system 所属 {@link ExtendedActorSystem}
     */
    public NettyTransport(Config conf, ExtendedActorSystem system) {
        this(system, conf);
    }

    @Override
    public String schemeIdentifier() {
        return settings.TransportMode;
    }

    @Override
    public boolean isResponsibleFor(Address address) {
        return true;
    }

    @Override
    public int maximumPayloadBytes() {
        return settings.MaxFrameSize;
    }

    @Override
    public CompletionStage<ListenResult> listen() {
        CompletableFuture<ListenResult> resultFuture = new CompletableFuture<>();
        CompletableFuture<AssociationEventListener> listenerPromise = new CompletableFuture<>();
        this.associationListenerPromise = listenerPromise;

        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, settings.Backlog)
                .option(ChannelOption.SO_REUSEADDR, settings.TcpReuseAddr)
                .childOption(ChannelOption.TCP_NODELAY, settings.TcpNodelay)
                .childOption(ChannelOption.SO_KEEPALIVE, settings.TcpKeepalive)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        configurePipeline(ch.pipeline());
                        ch.pipeline().addLast("handler", new ServerHandler(ch, listenerPromise));
                    }
                });
        if (settings.ReceiveBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, settings.ReceiveBufferSize);
        }
        if (settings.SendBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, settings.SendBufferSize);
        }
        if (settings.WriteBufferHighWaterMark > 0 && settings.WriteBufferLowWaterMark > 0) {
            bootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(settings.WriteBufferLowWaterMark, settings.WriteBufferHighWaterMark));
        }

        bootstrap.bind(settings.BindHostname, settings.BindPort).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                resultFuture.completeExceptionally(
                        new NettyTransportException("Failed to bind Netty server on "
                                + settings.BindHostname + ":" + settings.BindPort, f.cause()));
                return;
            }
            Channel serverChannel = f.channel();
            allChannels.add(serverChannel);
            InetSocketAddress socketAddr = (InetSocketAddress) serverChannel.localAddress();
            int actualPort = settings.Port == 0 ? socketAddr.getPort() : settings.Port;
            Address addr = new Address(
                    schemeIdentifier() + "." + ActorPath.Scheme,
                    system.settings().name(),
                    settings.Hostname,
                    actualPort);
            Address canonical = new Address(
                    schemeIdentifier(),
                    system.settings().name(),
                    settings.Hostname,
                    actualPort);
            this.boundAddress = addr;
            this.localAddress = canonical;
            resultFuture.complete(new ListenResult(canonical, listenerPromise));
        });
        return resultFuture;
    }

    @Override
    public CompletionStage<AssociationHandle> associate(Address remoteAddress) {
        if (remoteAddress.host() == null || remoteAddress.port() == null) {
            return CompletableFuture.failedFuture(new InvalidAssociationException(
                    "Address [" + remoteAddress + "] has no host or port", null));
        }
        String host = remoteAddress.host();
        int port = remoteAddress.port();
        CompletableFuture<AssociationHandle> future = new CompletableFuture<>();

        Bootstrap bootstrap = new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, settings.TcpNodelay)
                .option(ChannelOption.SO_KEEPALIVE, settings.TcpKeepalive)
                .option(ChannelOption.SO_REUSEADDR, settings.TcpReuseAddr)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.min(settings.ConnectionTimeout.toMillis(), Integer.MAX_VALUE))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        configurePipeline(ch.pipeline());
                        ch.pipeline().addLast("handler", new ClientHandler(ch, remoteAddress, future));
                    }
                });
        if (settings.ReceiveBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, settings.ReceiveBufferSize);
        }
        if (settings.SendBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, settings.SendBufferSize);
        }
        if (settings.WriteBufferHighWaterMark > 0 && settings.WriteBufferLowWaterMark > 0) {
            bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(settings.WriteBufferLowWaterMark, settings.WriteBufferHighWaterMark));
        }

        bootstrap.connect(host, port).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                future.completeExceptionally(new NettyTransportException(
                        "Association to [" + remoteAddress + "] failed", f.cause()));
            }
        });
        return future;
    }

    @Override
    public CompletionStage<Boolean> shutdown() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        allChannels.close().addListener(cf -> {
            serverGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
            future.complete(true);
        });
        return future;
    }

    private void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(settings.MaxFrameSize, 0, 4, 0, 4));
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
    }

    private Address resolveLocalAddress(Channel ch) {
        Address existing = this.localAddress;
        if (existing != null) {
            return existing;
        }
        InetSocketAddress sa = (InetSocketAddress) ch.localAddress();
        return new Address(schemeIdentifier(), system.settings().name(), sa.getAddress().getHostAddress(), sa.getPort());
    }

    private static Address mapRemoteAddress(Channel ch, String scheme, String systemName) {
        InetSocketAddress sa = (InetSocketAddress) ch.remoteAddress();
        return new Address(scheme, systemName, sa.getAddress().getHostAddress(), sa.getPort());
    }

    private final class NettyAssociationHandle implements AssociationHandle {
        private final Address local;
        private final Address remote;
        private final Channel channel;
        private final CompletableFuture<HandleEventListener> readHandlerPromise = new CompletableFuture<>();
        private final Queue<HandleEvent> bufferedEvents = new ConcurrentLinkedQueue<>();
        private volatile HandleEventListener registeredListener;
        private final AtomicBoolean disassociated = new AtomicBoolean(false);

        NettyAssociationHandle(Address local, Address remote, Channel channel) {
            this.local = local;
            this.remote = remote;
            this.channel = channel;
            this.readHandlerPromise.whenComplete((listener, err) -> {
                if (err == null && listener != null) {
                    this.registeredListener = listener;
                    drainBuffered(listener);
                }
            });
        }

        private void drainBuffered(HandleEventListener listener) {
            HandleEvent ev;
            while ((ev = bufferedEvents.poll()) != null) {
                listener.notify(ev);
            }
        }

        void dispatchEvent(HandleEvent ev) {
            HandleEventListener listener = this.registeredListener;
            if (listener != null) {
                drainBuffered(listener);
                listener.notify(ev);
            } else {
                bufferedEvents.offer(ev);
                listener = this.registeredListener;
                if (listener != null) {
                    drainBuffered(listener);
                }
            }
        }

        @Override
        public Address localAddress() {
            return local;
        }

        @Override
        public Address remoteAddress() {
            return remote;
        }

        @Override
        public CompletableFuture<HandleEventListener> readHandlerPromise() {
            return readHandlerPromise;
        }

        @Override
        public boolean write(ByteString payload) {
            if (!channel.isActive() || !channel.isWritable()) {
                return false;
            }
            ByteBuf buf = Unpooled.wrappedBuffer(payload.asReadOnlyByteBuffer());
            channel.writeAndFlush(buf).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            return true;
        }

        @Override
        public void disassociate(String reason, LoggingAdapter logAdapter) {
            AssociationHandle.super.disassociate(reason, logAdapter);
            if (disassociated.compareAndSet(false, true)) {
                channel.close();
            }
        }
    }

    private abstract class CommonHandler extends SimpleChannelInboundHandler<ByteBuf> {
        protected final Channel channel;
        protected volatile NettyAssociationHandle handle;

        CommonHandler(Channel channel) {
            this.channel = channel;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            byte[] bytes = new byte[msg.readableBytes()];
            msg.readBytes(bytes);
            NettyAssociationHandle h = this.handle;
            if (h != null) {
                h.dispatchEvent(new AssociationHandle.InboundPayload(ByteString.copyFrom(bytes)));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            NettyAssociationHandle h = this.handle;
            if (h != null) {
                h.dispatchEvent(new AssociationHandle.Disassociated(AssociationHandle.Unknown.INSTANCE));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("Netty transport exception on channel [{}]: {}", ctx.channel(), cause.getMessage());
            ctx.close();
        }
    }

    private final class ServerHandler extends CommonHandler {
        private final CompletableFuture<AssociationEventListener> listenerPromise;

        ServerHandler(Channel channel, CompletableFuture<AssociationEventListener> listenerPromise) {
            super(channel);
            this.listenerPromise = listenerPromise;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            allChannels.add(ctx.channel());
            Address local = resolveLocalAddress(ctx.channel());
            Address remote = mapRemoteAddress(ctx.channel(), schemeIdentifier(), system.settings().name());
            NettyAssociationHandle h = new NettyAssociationHandle(local, remote, ctx.channel());
            this.handle = h;
            ctx.channel().config().setAutoRead(false);
            listenerPromise.whenComplete((listener, err) -> {
                if (err == null && listener != null) {
                    listener.notify(new InboundAssociation(h));
                    h.readHandlerPromise().whenComplete((hl, hErr) -> {
                        if (hErr == null) {
                            ctx.channel().config().setAutoRead(true);
                        }
                    });
                } else {
                    ctx.close();
                }
            });
        }
    }

    private final class ClientHandler extends CommonHandler {
        private final Address remoteAddress;
        private final CompletableFuture<AssociationHandle> connectFuture;

        ClientHandler(Channel channel, Address remoteAddress, CompletableFuture<AssociationHandle> connectFuture) {
            super(channel);
            this.remoteAddress = remoteAddress;
            this.connectFuture = connectFuture;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            allChannels.add(ctx.channel());
            Address local = resolveLocalAddress(ctx.channel());
            NettyAssociationHandle h = new NettyAssociationHandle(local, remoteAddress, ctx.channel());
            this.handle = h;
            ctx.channel().config().setAutoRead(false);
            h.readHandlerPromise().whenComplete((hl, hErr) -> {
                if (hErr == null) {
                    ctx.channel().config().setAutoRead(true);
                }
            });
            connectFuture.complete(h);
        }
    }
}
