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
package io.github.espen7.tapio.remote;

import com.google.protobuf.ByteString;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.ActorSelectionMessage;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.remote.transport.AssociationHandle;
import io.github.espen7.tapio.remote.transport.TapioPduCodec;
import io.github.espen7.tapio.remote.transport.TapioProtocolHandle;
import io.github.espen7.tapio.remote.transport.TapioProtocolTransport;
import io.github.espen7.tapio.serialization.Serialization;

import java.io.NotSerializableException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * INTERNAL API：管理出站/入站远程关联、可靠系统消息投递、门控（Gated）与隔离（Quarantined）状态机。
 */
@InternalApi
public final class EndpointManager {
    private final ExtendedActorSystem system;
    private final RemoteActorRefProvider provider;
    private final RemoteSettings settings;
    private final LoggingAdapter log;
    private final EventPublisher eventPublisher;
    private final Map<String, TapioProtocolTransport> transportsByScheme = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Address, EndpointState> endpoints = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Address, QuarantinedEntry> quarantined = new ConcurrentHashMap<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private record QuarantinedEntry(long uid, Instant deadline) {
    }

    /**
     * 构造端点管理器。
     *
     * @param system 所属系统
     * @param provider 关联的 {@link RemoteActorRefProvider}
     * @param settings 远程配置
     * @param log 日志适配器
     * @param eventPublisher 生命周期事件发布器
     */
    public EndpointManager(
            ExtendedActorSystem system,
            RemoteActorRefProvider provider,
            RemoteSettings settings,
            LoggingAdapter log,
            EventPublisher eventPublisher) {
        this.system = system;
        this.provider = provider;
        this.settings = settings;
        this.log = log;
        this.eventPublisher = eventPublisher;
    }

    void registerTransport(String scheme, TapioProtocolTransport transport) {
        transportsByScheme.put(scheme, transport);
    }

    /**
     * 处理新建立的入站关联句柄。
     *
     * @param handle 入站协议句柄
     */
    public void acceptInbound(TapioProtocolHandle handle) {
        Address peerAddr = canonicalAddress(handle.remoteAddress());
        long peerUid = handle.handshakeInfo().uid();
        if (isQuarantined(peerAddr, peerUid)) {
            handle.disassociate(AssociationHandle.Quarantined.INSTANCE, "Inbound from quarantined UID", log);
            return;
        }
        EndpointState state = endpoints.computeIfAbsent(peerAddr, EndpointState::new);
        state.attachHandle(handle, true);
        eventPublisher.notifyListeners(new AssociatedEvent(
                canonicalAddress(handle.localAddress()), peerAddr, true));
    }

    /**
     * 发送远程消息。
     *
     * @param message 消息体
     * @param sender 发送方
     * @param recipient 接收方
     */
    public void send(Object message, ActorRef sender, RemoteActorRef recipient) {
        Address remoteAddr = canonicalAddress(recipient.path().address());
        if (shuttingDown.get() || isQuarantinedAddress(remoteAddr)) {
            system.deadLetters().tell(message, sender);
            return;
        }
        EndpointState state = endpoints.computeIfAbsent(remoteAddr, EndpointState::new);
        state.enqueue(message, sender, recipient);
    }

    /**
     * 隔离指定远端节点与可选 UID。
     *
     * @param address 远端地址
     * @param uidOpt 可选 UID
     * @param reason 隔离原因
     */
    public void quarantine(Address address, Optional<Long> uidOpt, String reason) {
        Address canonical = canonicalAddress(address);
        if (uidOpt.isPresent()) {
            long uid = uidOpt.get();
            quarantined.put(canonical, new QuarantinedEntry(uid, Instant.now().plus(settings.QuarantineDuration)));
            EndpointState state = endpoints.remove(canonical);
            if (state != null) {
                state.close(AssociationHandle.Quarantined.INSTANCE, reason);
            }
            eventPublisher.notifyListeners(new QuarantinedEvent(canonical, uid));
        } else {
            EndpointState state = endpoints.get(canonical);
            if (state != null) {
                state.gate(reason);
            }
        }
    }

    /**
     * 关闭所有端点与传输通道。
     */
    public void shutdown() {
        if (shuttingDown.compareAndSet(false, true)) {
            for (EndpointState state : endpoints.values()) {
                state.close(AssociationHandle.Shutdown.INSTANCE, "Remoting shutdown");
            }
            endpoints.clear();
        }
    }

    private boolean isQuarantinedAddress(Address addr) {
        QuarantinedEntry entry = quarantined.get(addr);
        if (entry == null) {
            return false;
        }
        if (Instant.now().isAfter(entry.deadline())) {
            quarantined.remove(addr, entry);
            return false;
        }
        return true;
    }

    private boolean isQuarantined(Address addr, long uid) {
        QuarantinedEntry entry = quarantined.get(addr);
        if (entry == null) {
            return false;
        }
        if (Instant.now().isAfter(entry.deadline())) {
            quarantined.remove(addr, entry);
            return false;
        }
        return entry.uid() == uid;
    }

    private static Address canonicalAddress(Address addr) {
        String proto = addr.protocol();
        int dot = proto.indexOf('.');
        if (dot > 0) {
            return new Address(proto.substring(0, dot), addr.system(), addr.host(), addr.port());
        }
        return addr;
    }

    private TapioProtocolTransport transportFor(Address remoteAddr) {
        for (TapioProtocolTransport t : transportsByScheme.values()) {
            if (t.isResponsibleFor(remoteAddr)) {
                return t;
            }
        }
        throw new RemoteTransportException("No transport available for [" + remoteAddr + "]", null);
    }

    private record OutboundItem(Object message, ActorRef sender, RemoteActorRef recipient, Long sysSeq) {
    }

    private final class EndpointState {
        private final Address remoteAddress;
        private final Queue<OutboundItem> pending = new ConcurrentLinkedQueue<>();
        private final AtomicLong nextSysSeq = new AtomicLong(1L);
        private final ConcurrentHashMap<Long, OutboundItem> unackedSysMsgs = new ConcurrentHashMap<>();
        private final AtomicLong lastDeliveredSysSeq = new AtomicLong(0L);
        private volatile TapioProtocolHandle activeHandle;
        private volatile boolean connecting = false;
        private volatile Instant gatedUntil = null;
        private volatile Cancellable retryTask;

        EndpointState(Address remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        void enqueue(Object message, ActorRef sender, RemoteActorRef recipient) {
            if (gatedUntil != null && Instant.now().isBefore(gatedUntil)) {
                if (!(message instanceof SystemMessage)) {
                    system.deadLetters().tell(message, sender);
                    return;
                }
            }
            Long seq = (message instanceof SystemMessage) ? nextSysSeq.getAndIncrement() : null;
            OutboundItem item = new OutboundItem(message, sender, recipient, seq);
            if (seq != null) {
                unackedSysMsgs.put(seq, item);
            }
            pending.offer(item);
            ensureConnectedAndFlush();
        }

        synchronized void attachHandle(TapioProtocolHandle handle, boolean inbound) {
            if (activeHandle == null || !inbound || !settings.UsePassiveConnections) {
                if (activeHandle != null && !inbound) {
                    activeHandle.disassociate("Replaced by new outbound handle", log);
                }
                activeHandle = handle;
                connecting = false;
                gatedUntil = null;
                handle.readHandlerPromise().complete(ev -> handleInboundEvent(handle, ev));
                resendUnacked();
                flushPending();
            } else {
                handle.readHandlerPromise().complete(ev -> handleInboundEvent(handle, ev));
            }
        }

        private void handleInboundEvent(TapioProtocolHandle handle, AssociationHandle.HandleEvent ev) {
            switch (ev) {
                case AssociationHandle.InboundPayload ip -> {
                    TapioPduCodec.DecodedContainer decoded = TapioPduCodec.decodeMessage(
                            ip.payload(), provider, canonicalAddress(handle.localAddress()));
                    decoded.ack().ifPresent(this::processAck);
                    decoded.message().ifPresent(msg -> dispatchInbound(handle, msg));
                }
                case AssociationHandle.Disassociated d -> {
                    if (d.info() instanceof AssociationHandle.Quarantined) {
                        eventPublisher.notifyListeners(new ThisActorSystemQuarantinedEvent(
                                canonicalAddress(handle.localAddress()), remoteAddress));
                        quarantined.put(remoteAddress, new QuarantinedEntry(
                                handle.handshakeInfo().uid(), Instant.now().plus(settings.QuarantineDuration)));
                    }
                    if (this.activeHandle == handle) {
                        this.activeHandle = null;
                        this.connecting = false;
                    }
                    eventPublisher.notifyListeners(new DisassociatedEvent(
                            canonicalAddress(handle.localAddress()), remoteAddress, false));
                }
            }
        }

        private void processAck(WireFormats.AcknowledgementInfo ack) {
            long cum = ack.cumulativeAck();
            unackedSysMsgs.keySet().removeIf(seq -> seq <= cum);
            for (long nack : ack.nacks()) {
                OutboundItem item = unackedSysMsgs.get(nack);
                if (item != null) {
                    pending.offer(item);
                }
            }
            flushPending();
        }

        private void dispatchInbound(TapioProtocolHandle handle, TapioPduCodec.DecodedMessage msg) {
            if (msg.seq().isPresent()) {
                long seq = msg.seq().get();
                long expected = lastDeliveredSysSeq.get() + 1;
                if (seq < expected) {
                    sendPureAck(handle, lastDeliveredSysSeq.get(), List.of());
                    return;
                }
                lastDeliveredSysSeq.compareAndSet(expected - 1, seq);
                sendPureAck(handle, seq, List.of());
            }
            Object payload;
            try {
                payload = Serialization.withTransportInformation(system, () -> {
                    try {
                        return MessageSerializer.deserialize(system, msg.serializedMessage());
                    } catch (NotSerializableException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                log.warning("Failed to deserialize inbound message from [{}]: {}", remoteAddress, e.getMessage());
                return;
            }

            ActorRef sender = msg.senderOption().orElse(system.deadLetters());
            InternalActorRef recipient = msg.recipient();
            if (recipient == provider.remoteDaemon()) {
                if (!settings.UntrustedMode) {
                    provider.remoteDaemon().tell(payload, sender);
                }
                return;
            }
            if (payload instanceof ActorSelectionMessage sel) {
                ActorSelection.deliverSelection(recipient, sender, sel);
            } else if (payload instanceof SystemMessage sysMsg) {
                recipient.sendSystemMessage(sysMsg);
            } else {
                recipient.tell(payload, sender);
            }
        }

        private void sendPureAck(TapioProtocolHandle handle, long ackSeq, List<Long> nacks) {
            ByteString ackPayload = TapioPduCodec.constructPureAck(
                    new WireFormats.AcknowledgementInfo(ackSeq, nacks));
            handle.write(ackPayload);
        }

        private synchronized void ensureConnectedAndFlush() {
            if (activeHandle != null) {
                flushPending();
                return;
            }
            if (connecting || (gatedUntil != null && Instant.now().isBefore(gatedUntil))) {
                return;
            }
            connecting = true;
            Optional<Long> refuseUid = Optional.ofNullable(quarantined.get(remoteAddress))
                    .map(QuarantinedEntry::uid);
            TapioProtocolTransport transport = transportFor(remoteAddress);
            transport.associate(remoteAddress, refuseUid).whenComplete((handle, err) -> {
                if (err != null) {
                    connecting = false;
                    gate("Association failed: " + err.getMessage());
                    eventPublisher.notifyListeners(new AssociationErrorEvent(
                            err, provider.getDefaultAddress(), remoteAddress, false,
                            io.github.espen7.tapio.event.LogEvent.Level.WARNING));
                    return;
                }
                attachHandle(handle, false);
                eventPublisher.notifyListeners(new AssociatedEvent(
                        canonicalAddress(handle.localAddress()), remoteAddress, false));
            });
        }

        private void resendUnacked() {
            if (!unackedSysMsgs.isEmpty()) {
                List<Long> keys = new ArrayList<>(unackedSysMsgs.keySet());
                keys.sort(Long::compareTo);
                for (Long k : keys) {
                    OutboundItem item = unackedSysMsgs.get(k);
                    if (item != null) {
                        pending.offer(item);
                    }
                }
            }
        }

        private void flushPending() {
            TapioProtocolHandle handle = this.activeHandle;
            if (handle == null) {
                return;
            }
            OutboundItem item;
            while ((item = pending.poll()) != null) {
                ByteString container;
                try {
                    OutboundItem current = item;
                    container = Serialization.withTransportInformation(system, () -> {
                        try {
                            WireFormats.SerializedMessage serialized = MessageSerializer.serialize(system, current.message());
                            return TapioPduCodec.constructMessage(
                                    canonicalAddress(handle.localAddress()),
                                    current.recipient(),
                                    serialized,
                                    Optional.ofNullable(current.sender()),
                                    Optional.ofNullable(current.sysSeq()),
                                    Optional.empty());
                        } catch (NotSerializableException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (RuntimeException e) {
                    log.error(e, "Failed to serialize outbound message [{}] to [{}]",
                            item.message().getClass().getName(), remoteAddress);
                    continue;
                }
                if (!handle.write(container)) {
                    pending.offer(item);
                    scheduleBackoffFlush();
                    return;
                }
            }
        }

        private void scheduleBackoffFlush() {
            if (retryTask == null) {
                retryTask = system.scheduler().scheduleOnce(settings.BackoffPeriod, () -> {
                    retryTask = null;
                    flushPending();
                }, system.dispatcher());
            }
        }

        synchronized void gate(String reason) {
            this.gatedUntil = Instant.now().plus(settings.RetryGateClosedFor);
            if (activeHandle != null) {
                activeHandle.disassociate(reason, log);
                activeHandle = null;
            }
            OutboundItem item;
            while ((item = pending.poll()) != null) {
                if (!(item.message() instanceof SystemMessage)) {
                    system.deadLetters().tell(item.message(), item.sender());
                }
            }
        }

        synchronized void close(AssociationHandle.DisassociateInfo info, String reason) {
            if (activeHandle != null) {
                activeHandle.disassociate(info, reason, log);
                activeHandle = null;
            }
            OutboundItem item;
            while ((item = pending.poll()) != null) {
                system.deadLetters().tell(item.message(), item.sender());
            }
            unackedSysMsgs.clear();
        }
    }
}
