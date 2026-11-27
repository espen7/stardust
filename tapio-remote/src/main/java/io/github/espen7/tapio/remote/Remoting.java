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

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.remote.transport.TapioProtocolHandle;
import io.github.espen7.tapio.remote.transport.TapioProtocolSettings;
import io.github.espen7.tapio.remote.transport.TapioProtocolTransport;
import io.github.espen7.tapio.remote.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * INTERNAL API：Classic Remoting 核心传输协调器，管理底层传输启动、地址映射与端点管理器。
 */
@InternalApi
public final class Remoting extends RemoteTransport {
    private final LoggingAdapter log;
    private final EventPublisher eventPublisher;
    private final EndpointManager endpointManager;
    private final List<TapioProtocolTransport> transports = new ArrayList<>();
    private final Map<String, Set<Address>> transportMapping = new HashMap<>();
    private volatile Set<Address> addresses = Set.of();
    private volatile Address defaultAddress;

    /**
     * 构造 Classic Remoting 实例。
     *
     * @param system 所属 {@link ExtendedActorSystem}
     * @param provider 关联的 {@link RemoteActorRefProvider}
     */
    public Remoting(ExtendedActorSystem system, RemoteActorRefProvider provider) {
        super(system, provider);
        this.log = Logging.getLogger(system, this, getClass());
        RemoteSettings settings = provider.remoteSettings();
        this.eventPublisher = new EventPublisher(system, log, settings.RemoteLifecycleEventsLogLevel);
        this.endpointManager = new EndpointManager(system, provider, settings, log, eventPublisher);
    }

    @Override
    public LoggingAdapter log() {
        return log;
    }

    @Override
    public long systemUid() {
        return AddressUidExtension.INSTANCE.get(system).longAddressUid();
    }

    @Override
    public Set<Address> addresses() {
        return addresses;
    }

    @Override
    public Address defaultAddress() {
        return defaultAddress;
    }

    @Override
    public Address localAddressForRemote(Address remote) {
        Set<Address> candidates = transportMapping.get(remote.protocol());
        if (candidates == null || candidates.isEmpty()) {
            if (defaultAddress != null) {
                return defaultAddress;
            }
            throw new RemoteTransportException(
                    "No transport is loaded for protocol: [" + remote.protocol() + "]", null);
        }
        return candidates.iterator().next();
    }

    @Override
    public void start() {
        RemoteSettings settings = provider.remoteSettings();
        TapioProtocolSettings protoSettings = new TapioProtocolSettings(system.settings().config());
        Set<Address> bound = new HashSet<>();
        for (RemoteSettings.TransportConfig tc : settings.Transports) {
            Transport raw = instantiateTransport(tc.transportClass(), tc.config());
            TapioProtocolTransport protoTransport = new TapioProtocolTransport(raw, system, protoSettings);
            transports.add(protoTransport);
            endpointManager.registerTransport(protoTransport.schemeIdentifier(), protoTransport);
            try {
                Transport.ListenResult lr = protoTransport.listen()
                        .toCompletableFuture()
                        .get(settings.StartupTimeout.toMillis(), TimeUnit.MILLISECONDS);
                Address publicAddr = toPublicAddress(lr.listenAddress(), settings.ProtocolName);
                bound.add(publicAddr);
                transportMapping.computeIfAbsent(settings.ProtocolName, ignored -> new HashSet<>()).add(publicAddr);
                if (this.defaultAddress == null) {
                    this.defaultAddress = publicAddr;
                }
                lr.listenerPromise().complete(ev -> {
                    if (ev instanceof Transport.InboundAssociation ia
                            && ia.association() instanceof TapioProtocolHandle handle) {
                        endpointManager.acceptInbound(handle);
                    }
                });
            } catch (Exception e) {
                throw new RemoteTransportException("Failed to start transport [" + tc.transportClass() + "]", e);
            }
        }
        this.addresses = Collections.unmodifiableSet(bound);
        eventPublisher.notifyListeners(new RemotingListenEvent(this.addresses));
    }

    private static Address toPublicAddress(Address addr, String scheme) {
        return new Address(scheme, addr.system(), addr.host(), addr.port());
    }

    private Transport instantiateTransport(String fqcn, Config conf) {
        try {
            return system.dynamicAccess().createInstanceFor(
                    fqcn,
                    Transport.class,
                    List.of(Map.entry(ExtendedActorSystem.class, system), Map.entry(Config.class, conf)));
        } catch (Throwable t) {
            throw new ConfigurationException("Cannot instantiate Transport [" + fqcn + "]", t);
        }
    }

    @Override
    public void send(Object message, ActorRef sender, RemoteActorRef recipient) {
        endpointManager.send(message, sender, recipient);
    }

    @Override
    public void quarantine(Address address, Optional<Long> uid, String reason) {
        endpointManager.quarantine(address, uid, reason);
    }

    @Override
    public CompletionStage<Done> shutdown() {
        endpointManager.shutdown();
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (TapioProtocolTransport t : transports) {
            futures.add(t.shutdown().toCompletableFuture());
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    eventPublisher.notifyListeners(RemotingShutdownEvent.INSTANCE);
                    return Done.getInstance();
                });
    }
}
