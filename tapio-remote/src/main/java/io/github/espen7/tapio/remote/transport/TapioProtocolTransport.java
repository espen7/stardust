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
package io.github.espen7.tapio.remote.transport;

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.remote.AddressUidExtension;
import io.github.espen7.tapio.remote.RemoteSettings;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * INTERNAL API：在底层 {@link Transport} 之上实现 Tapio 握手、心跳与协议级 Disassociate 的传输适配器。
 */
@InternalApi
public final class TapioProtocolTransport implements Transport {
    private final Transport wrappedTransport;
    private final ExtendedActorSystem system;
    private final TapioProtocolSettings settings;
    private final LoggingAdapter log;
    private final String addedSchemeIdentifier;
    private final long localUid;
    private volatile Address localListenAddress;

    /**
     * 构造协议传输适配器。
     *
     * @param wrappedTransport 底层传输实例
     * @param system 所属 {@link ExtendedActorSystem}
     * @param settings 协议配置
     */
    public TapioProtocolTransport(
            Transport wrappedTransport,
            ExtendedActorSystem system,
            TapioProtocolSettings settings) {
        this.wrappedTransport = wrappedTransport;
        this.system = system;
        this.settings = settings;
        this.log = Logging.getLogger(system, this, getClass());
        this.addedSchemeIdentifier = new RemoteSettings(system.settings().config()).ProtocolName;
        this.localUid = AddressUidExtension.INSTANCE.get(system).longAddressUid();
    }

    @Override
    public String schemeIdentifier() {
        return addedSchemeIdentifier + "." + wrappedTransport.schemeIdentifier();
    }

    @Override
    public boolean isResponsibleFor(Address address) {
        return wrappedTransport.isResponsibleFor(address);
    }

    @Override
    public int maximumPayloadBytes() {
        return wrappedTransport.maximumPayloadBytes();
    }

    @Override
    public CompletionStage<ListenResult> listen() {
        return wrappedTransport.listen().thenApply(lr -> {
            Address exposed = withAddedScheme(lr.listenAddress());
            this.localListenAddress = exposed;
            CompletableFuture<AssociationEventListener> upperPromise = new CompletableFuture<>();
            lr.listenerPromise().complete(ev -> {
                if (ev instanceof InboundAssociation ia) {
                    handleInbound(ia.association(), upperPromise);
                }
            });
            return new ListenResult(exposed, upperPromise);
        });
    }

    @Override
    public CompletionStage<AssociationHandle> associate(Address remoteAddress) {
        return associate(remoteAddress, Optional.empty()).thenApply(h -> h);
    }

    /**
     * 发起带可选拒绝 UID 校验的出站关联，并完成 Tapio 握手。
     *
     * @param remoteAddress 远端地址
     * @param refuseUid 若非空且对端握手 UID 与之相等则拒绝关联
     * @return 完成握手后的 {@link TapioProtocolHandle}
     */
    public CompletionStage<TapioProtocolHandle> associate(Address remoteAddress, Optional<Long> refuseUid) {
        Address stripped = removeScheme(remoteAddress);
        CompletableFuture<TapioProtocolHandle> handshakePromise = new CompletableFuture<>();
        wrappedTransport.associate(stripped).whenComplete((rawHandle, err) -> {
            if (err != null) {
                handshakePromise.completeExceptionally(err);
                return;
            }
            Address local = this.localListenAddress != null
                    ? this.localListenAddress
                    : withAddedScheme(rawHandle.localAddress());
            Address remote = withAddedScheme(remoteAddress);
            rawHandle.write(TapioPduCodec.constructAssociate(new TapioPduCodec.HandshakeInfo(local, localUid)));
            attachProtocolState(rawHandle, local, remote, false, refuseUid, handshakePromise, null);
        });
        return handshakePromise;
    }

    private void handleInbound(
            AssociationHandle rawHandle,
            CompletableFuture<AssociationEventListener> upperListenerPromise) {
        Address local = this.localListenAddress != null
                ? this.localListenAddress
                : withAddedScheme(rawHandle.localAddress());
        attachProtocolState(rawHandle, local, withAddedScheme(rawHandle.remoteAddress()), true,
                Optional.empty(), null, upperListenerPromise);
    }

    private void attachProtocolState(
            AssociationHandle rawHandle,
            Address local,
            Address remoteInitial,
            boolean inbound,
            Optional<Long> refuseUid,
            CompletableFuture<TapioProtocolHandle> outboundPromise,
            CompletableFuture<AssociationEventListener> inboundListenerPromise) {
        CompletableFuture<AssociationHandle.HandleEventListener> upperReadPromise = new CompletableFuture<>();
        Queue<AssociationHandle.HandleEvent> buffer = new ConcurrentLinkedQueue<>();
        AtomicBoolean handshakeDone = new AtomicBoolean(false);
        AtomicBoolean closed = new AtomicBoolean(false);
        final TapioProtocolHandle[] handleHolder = new TapioProtocolHandle[1];

        Cancellable timeoutTask = system.scheduler().scheduleOnce(
                settings.HandshakeTimeout,
                () -> {
                    if (!handshakeDone.get() && closed.compareAndSet(false, true)) {
                        rawHandle.disassociate("Handshake timeout", log);
                        if (outboundPromise != null) {
                            outboundPromise.completeExceptionally(
                                    new IllegalStateException("Handshake timeout with [" + remoteInitial + "]"));
                        }
                    }
                },
                system.dispatcher());

        Cancellable heartbeatTask = system.scheduler().scheduleWithFixedDelay(
                settings.TransportHeartBeatInterval,
                settings.TransportHeartBeatInterval,
                () -> {
                    if (handshakeDone.get() && !closed.get()) {
                        rawHandle.write(TapioPduCodec.constructHeartbeat());
                    }
                },
                system.dispatcher());

        upperReadPromise.whenComplete((listener, err) -> {
            if (err == null && listener != null) {
                AssociationHandle.HandleEvent ev;
                while ((ev = buffer.poll()) != null) {
                    listener.notify(ev);
                }
            }
        });

        rawHandle.readHandlerPromise().complete(ev -> {
            if (ev instanceof AssociationHandle.Disassociated d) {
                timeoutTask.cancel();
                heartbeatTask.cancel();
                if (!handshakeDone.get() && outboundPromise != null) {
                    outboundPromise.completeExceptionally(
                            new IllegalStateException("Association disassociated during handshake: " + d.info()));
                }
                deliverHandleEvent(d, upperReadPromise, buffer);
                return;
            }
            if (ev instanceof AssociationHandle.InboundPayload ip) {
                TapioPduCodec.TapioPdu pdu = TapioPduCodec.decodePdu(ip.payload());
                switch (pdu) {
                    case TapioPduCodec.Associate assoc -> {
                        timeoutTask.cancel();
                        TapioPduCodec.HandshakeInfo info = assoc.info();
                        if (refuseUid.isPresent() && refuseUid.get() == info.uid()) {
                            closed.set(true);
                            heartbeatTask.cancel();
                            rawHandle.write(TapioPduCodec.constructDisassociate(AssociationHandle.Quarantined.INSTANCE));
                            rawHandle.disassociate("Refused UID [" + info.uid() + "]", log);
                            if (outboundPromise != null) {
                                outboundPromise.completeExceptionally(
                                        new IllegalStateException("Refused quarantined UID [" + info.uid() + "]"));
                            }
                            return;
                        }
                        if (inbound) {
                            rawHandle.write(TapioPduCodec.constructAssociate(
                                    new TapioPduCodec.HandshakeInfo(local, localUid)));
                        }
                        Address peerAddr = withAddedScheme(info.origin());
                        TapioProtocolHandle protoHandle = new TapioProtocolHandle(
                                local, peerAddr, upperReadPromise, rawHandle, info);
                        handleHolder[0] = protoHandle;
                        handshakeDone.set(true);
                        if (outboundPromise != null) {
                            outboundPromise.complete(protoHandle);
                        } else if (inboundListenerPromise != null) {
                            inboundListenerPromise.whenComplete((listener, err) -> {
                                if (err == null && listener != null) {
                                    listener.notify(new InboundAssociation(protoHandle));
                                } else {
                                    protoHandle.disassociate("No inbound listener", log);
                                }
                            });
                        }
                    }
                    case TapioPduCodec.Disassociate dis -> {
                        timeoutTask.cancel();
                        heartbeatTask.cancel();
                        closed.set(true);
                        if (!handshakeDone.get() && outboundPromise != null) {
                            outboundPromise.completeExceptionally(
                                    new IllegalStateException("Handshake rejected with " + dis.reason()));
                        }
                        deliverHandleEvent(new AssociationHandle.Disassociated(dis.reason()), upperReadPromise, buffer);
                        rawHandle.disassociate("Remote disassociated: " + dis.reason(), log);
                    }
                    case TapioPduCodec.Heartbeat ignored -> {
                    }
                    case TapioPduCodec.Payload p -> {
                        deliverHandleEvent(new AssociationHandle.InboundPayload(p.bytes()), upperReadPromise, buffer);
                    }
                }
            }
        });
    }

    private static void deliverHandleEvent(
            AssociationHandle.HandleEvent ev,
            CompletableFuture<AssociationHandle.HandleEventListener> upperReadPromise,
            Queue<AssociationHandle.HandleEvent> buffer) {
        if (upperReadPromise.isDone() && !upperReadPromise.isCompletedExceptionally()) {
            AssociationHandle.HandleEventListener listener = upperReadPromise.join();
            AssociationHandle.HandleEvent queued;
            while ((queued = buffer.poll()) != null) {
                listener.notify(queued);
            }
            listener.notify(ev);
        } else {
            buffer.offer(ev);
            if (upperReadPromise.isDone() && !upperReadPromise.isCompletedExceptionally()) {
                AssociationHandle.HandleEventListener listener = upperReadPromise.join();
                AssociationHandle.HandleEvent queued;
                while ((queued = buffer.poll()) != null) {
                    listener.notify(queued);
                }
            }
        }
    }

    @Override
    public CompletionStage<Boolean> shutdown() {
        return wrappedTransport.shutdown();
    }

    @Override
    public CompletionStage<Boolean> managementCommand(Object cmd) {
        return wrappedTransport.managementCommand(cmd);
    }

    private Address withAddedScheme(Address addr) {
        if (addr.protocol().startsWith(addedSchemeIdentifier + ".")) {
            return addr;
        }
        if (addedSchemeIdentifier.equals(addr.protocol())) {
            return new Address(schemeIdentifier(), addr.system(), addr.host(), addr.port());
        }
        return new Address(
                addedSchemeIdentifier + "." + addr.protocol(),
                addr.system(),
                addr.host(),
                addr.port());
    }

    private static Address removeScheme(Address addr) {
        String proto = addr.protocol();
        int dot = proto.indexOf('.');
        if (dot > 0) {
            return new Address(proto.substring(dot + 1), addr.system(), addr.host(), addr.port());
        }
        return addr;
    }
}
