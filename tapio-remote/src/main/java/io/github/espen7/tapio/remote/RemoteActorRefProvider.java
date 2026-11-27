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
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.Deployer;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.LocalActorRef;
import io.github.espen7.tapio.actor.LocalActorRefProvider;
import io.github.espen7.tapio.actor.Nobody;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.RootActorPath;
import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.actor.SystemGuardian;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.serialization.Serialization;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * 支持远程通信的 {@link ActorRefProvider} 实现。
 */
public class RemoteActorRefProvider extends ActorRefProvider {
    private final ActorSystemImpl system;
    private final LocalActorRefProvider local;
    private final RemoteSettings remoteSettings;
    private final LoggingAdapter log;
    private volatile Remoting transport;
    private volatile RemoteDaemon remoteDaemon;
    private volatile ActorRef remoteWatcher;
    private volatile Serialization.Information serializationInformationCache;

    /**
     * 构造 {@link RemoteActorRefProvider}（由 {@link ActorSystemImpl} 通过反射实例化）。
     *
     * @param system 所属 {@link ActorSystemImpl}
     */
    public RemoteActorRefProvider(ActorSystemImpl system) {
        this.system = system;
        this.local = new LocalActorRefProvider(system);
        this.remoteSettings = new RemoteSettings(system.settings().config());
        this.log = Logging.getLogger(system, this, getClass());
    }

    /**
     * 返回远程配置视图。
     *
     * @return {@link RemoteSettings}
     */
    public RemoteSettings remoteSettings() {
        return remoteSettings;
    }

    /**
     * 返回底层 {@link RemoteTransport}。
     *
     * @return 远程传输实例
     */
    @InternalApi
    public RemoteTransport transport() {
        return transport;
    }

    /**
     * 返回本地 {@code /remote} 守护容器引用。
     *
     * @return {@link RemoteDaemon}
     */
    @InternalApi
    public RemoteDaemon remoteDaemon() {
        return remoteDaemon;
    }

    /**
     * 返回本系统的 {@link RemoteWatcher} 引用。
     *
     * @return {@link RemoteWatcher} ActorRef
     */
    @InternalApi
    public ActorRef remoteWatcher() {
        return remoteWatcher;
    }

    @Override
    public InternalActorRef rootGuardian() {
        return local.rootGuardian();
    }

    @Override
    public ActorRef rootGuardianAt(Address address) {
        if (hasAddress(address)) {
            return local.rootGuardian();
        }
        return new RemoteActorRef(transport, localAddressForRemote(address), new RootActorPath(address, "/"), Nobody.INSTANCE);
    }

    @Override
    public LocalActorRef guardian() {
        return local.guardian();
    }

    @Override
    public LocalActorRef systemGuardian() {
        return local.systemGuardian();
    }

    @Override
    public ActorRef deadLetters() {
        return local.deadLetters();
    }

    @Override
    @InternalApi
    public ActorRef ignoreRef() {
        return local.ignoreRef();
    }

    @Override
    public ActorPath rootPath() {
        return local.rootPath();
    }

    @Override
    public ActorSystem.Settings settings() {
        return local.settings();
    }

    @Override
    @InternalApi
    public void init(ActorSystemImpl sys) {
        local.init(sys);
        Remoting remoting = new Remoting(sys, this);
        this.transport = remoting;
        remoting.start();

        ActorRef terminator = new MinimalTerminator(remoting);
        this.remoteDaemon = new RemoteDaemon(
                sys,
                new RootActorPath(remoting.defaultAddress(), "/remote"),
                local.rootGuardian(),
                (InternalActorRef) terminator,
                log);

        FailureDetectorRegistry<Address> fdRegistry = new DefaultFailureDetectorRegistry<>(
                () -> createFailureDetector(
                        remoteSettings.WatchFailureDetectorImplementationClass,
                        remoteSettings.WatchFailureDetectorConfig,
                        sys.eventStream()));
        this.remoteWatcher = createRemoteWatcher(sys, fdRegistry);
        local.systemGuardian().tell(SystemGuardian.RegisterTerminationHook.INSTANCE, terminator);
    }

    protected ActorRef createRemoteWatcher(ActorSystemImpl sys, FailureDetectorRegistry<Address> fdRegistry) {
        return sys.systemActorOf(RemoteWatcher.props(remoteSettings, fdRegistry), "remote-watcher");
    }

    private FailureDetector createFailureDetector(String fqcn, Config cfg, EventStream ev) {
        try {
            return system.dynamicAccess().createInstanceFor(
                    fqcn,
                    FailureDetector.class,
                    List.of(Map.entry(Config.class, cfg), Map.entry(EventStream.class, ev)));
        } catch (Throwable t) {
            throw new ConfigurationException("Cannot instantiate FailureDetector [" + fqcn + "]", t);
        }
    }

    @Override
    public Deployer deployer() {
        return local.deployer();
    }

    @Override
    public Scheduler scheduler() {
        return local.scheduler();
    }

    @Override
    public ActorPath tempPath(String prefix) {
        return local.tempPath(prefix);
    }

    @Override
    public InternalActorRef tempContainer() {
        return local.tempContainer();
    }

    @Override
    public void registerTempActor(InternalActorRef actorRef, ActorPath path) {
        local.registerTempActor(actorRef, path);
    }

    @Override
    public void unregisterTempActor(ActorPath path) {
        local.unregisterTempActor(path);
    }

    @Override
    @InternalApi
    public InternalActorRef actorOf(
            ActorSystemImpl sys,
            Props props,
            InternalActorRef supervisor,
            ActorPath path,
            boolean systemService,
            Optional<Deploy> deploy,
            boolean lookupDeploy,
            boolean async) {
        if (systemService) {
            return local.actorOf(sys, props, supervisor, path, true, deploy, lookupDeploy, async);
        }
        Deploy effectiveDeploy = deploy.orElse(props.deploy());
        if (effectiveDeploy.scope() instanceof RemoteScope(Address node) && !hasAddress(node)) {
            ActorPath remotePath = new RootActorPath(node, "/remote").descendant(
                    List.of(path.toStringWithoutAddress().split("/")));
            RemoteActorRef remoteRef = new RemoteActorRef(
                    transport, localAddressForRemote(node), remotePath, supervisor);
            ActorRef peerDaemon = resolveActorRef(new RootActorPath(node, "/remote"));
            peerDaemon.tell(new DaemonMsgCreate(props, effectiveDeploy, remotePath.toString(), supervisor), supervisor);
            return remoteRef;
        }
        return local.actorOf(sys, props, supervisor, path, false, deploy, lookupDeploy, async);
    }

    @Override
    public ActorRef resolveActorRef(String path) {
        try {
            return resolveActorRef(ActorPath.fromString(path));
        } catch (java.net.MalformedURLException e) {
            return deadLetters();
        }
    }

    @Override
    public ActorRef resolveActorRef(ActorPath path) {
        if (hasAddress(path.address())) {
            List<String> elements = path.elements();
            if (elements.isEmpty()) {
                return local.rootGuardian();
            }
            if ("remote".equals(elements.getFirst())) {
                return remoteDaemon.getChild(elements.subList(1, elements.size()).iterator());
            }
            return local.resolveActorRef(local.rootGuardian(), elements);
        }
        return new RemoteActorRef(transport, localAddressForRemote(path.address()), path, Nobody.INSTANCE);
    }

    @Override
    public CompletionStage<Terminated> terminationFuture() {
        return local.terminationFuture();
    }

    @Override
    public Optional<Address> getExternalAddressFor(Address address) {
        if (hasAddress(address)) {
            return Optional.of(getDefaultAddress());
        }
        if (transport != null && transport.addresses().stream()
                .anyMatch(a -> a.protocol().equals(address.protocol()))) {
            return Optional.of(transport.localAddressForRemote(address));
        }
        return Optional.empty();
    }

    @Override
    public Address getDefaultAddress() {
        return transport != null && transport.defaultAddress() != null
                ? transport.defaultAddress()
                : local.getDefaultAddress();
    }

    @Override
    @InternalApi
    public Serialization.Information serializationInformation() {
        if (serializationInformationCache == null && transport != null && transport.defaultAddress() != null) {
            serializationInformationCache = new Serialization.Information(getDefaultAddress(), system);
        }
        return serializationInformationCache != null ? serializationInformationCache : local.serializationInformation();
    }

    @Override
    @InternalApi
    public String addressString() {
        return getDefaultAddress().toString();
    }

    @Override
    @InternalApi
    public long systemUid() {
        return AddressUidExtension.INSTANCE.get(system).longAddressUid();
    }

    /**
     * 隔离远端节点。
     *
     * @param address 远端地址
     * @param uid 可选 UID
     * @param reason 原因
     */
    public void quarantine(Address address, Optional<Long> uid, String reason) {
        if (transport != null) {
            transport.quarantine(address, uid, reason);
        }
    }

    void warnIfUnsafeDeathwatchWithoutCluster(InternalActorRef watchee, InternalActorRef watcher, String op) {
        if (remoteSettings.WarnUnsafeWatchWithoutCluster) {
            log.warning(
                    "Using remote {} without Cluster between [{}] and [{}] is not recommended. "
                            + "Set `tapio.remote.use-unsafe-remote-features-outside-cluster = on` to enable.",
                    op, watcher, watchee);
        }
    }

    private boolean hasAddress(Address address) {
        return address.equals(local.rootPath().address())
                || (transport != null && transport.addresses().contains(address));
    }

    private Address localAddressForRemote(Address remote) {
        return transport != null ? transport.localAddressForRemote(remote) : getDefaultAddress();
    }

    private final class MinimalTerminator extends io.github.espen7.tapio.actor.MinimalActorRef {
        private final Remoting remoting;

        MinimalTerminator(Remoting remoting) {
            this.remoting = remoting;
        }

        @Override
        public ActorPath path() {
            return local.rootPath().child("remoting-terminator");
        }

        @Override
        public ActorRefProvider provider() {
            return RemoteActorRefProvider.this;
        }

        @Override
        public InternalActorRef getParent() {
            return local.systemGuardian();
        }

        @Override
        public void tell(Object message, ActorRef sender) {
            if (message == SystemGuardian.TerminationHook.INSTANCE) {
                remoteDaemon.tell(SystemGuardian.TerminationHook.INSTANCE, this);
            } else if (message == SystemGuardian.TerminationHookDone.INSTANCE) {
                remoting.shutdown().whenComplete((done, err) ->
                        local.systemGuardian().tell(SystemGuardian.TerminationHookDone.INSTANCE, this));
            }
        }
    }
}
