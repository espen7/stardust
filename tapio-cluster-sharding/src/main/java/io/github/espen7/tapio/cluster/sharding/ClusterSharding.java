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
package io.github.espen7.tapio.cluster.sharding;

import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.PoisonPill;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Status;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterSettings;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.ReplicatorSettings;
import io.github.espen7.tapio.cluster.sharding.internal.DDataRememberEntitiesProvider;
import io.github.espen7.tapio.cluster.sharding.internal.LeastShardAllocationStrategy;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManager;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManagerSettings;
import io.github.espen7.tapio.coordination.lease.LeaseUsageSettings;
import io.github.espen7.tapio.pattern.Patterns;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 集群分片扩展入口（基于 DData 模式），通过内部 {@code ClusterShardingGuardian} Actor 托管分片区域与协调器单例。
 */
public final class ClusterSharding implements Extension {

    /** 扩展 ID。 */
    public static final AbstractExtensionId<ClusterSharding> Id = new AbstractExtensionId<>() {
        @Override
        public ClusterSharding createExtension(ExtendedActorSystem system) {
            return new ClusterSharding(system);
        }
    };

    /**
     * 获取或创建关联的 {@link ClusterSharding} 扩展实例。
     */
    public static ClusterSharding get(ActorSystem system) {
        return Id.apply(system);
    }

    /** INTERNAL API：Guardian 启动 Region 命令。 */
    @InternalApi
    record Start(
            String typeName,
            Function<String, Props> entityPropsFactory,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor,
            ShardAllocationStrategy allocationStrategy,
            Object handOffStopMessage) {
    }

    /** INTERNAL API：Guardian 启动 Proxy 命令。 */
    @InternalApi
    record StartProxy(
            String typeName,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor) {
    }

    /** INTERNAL API：Guardian 启动结果。 */
    @InternalApi
    record Started(ActorRef shardRegion) {
    }

    /**
     * INTERNAL API：串行管理按角色隔离的 {@link Replicator}、{@link ShardCoordinator} 单例与 {@link ShardRegion} 的 Guardian Actor。
     */
    @InternalApi
    static final class ClusterShardingGuardian extends AbstractLoggingActor {
        private final Cluster cluster;
        private final int majorityMinCap;
        private final Map<Optional<String>, ActorRef> replicatorByRole = new HashMap<>();

        ClusterShardingGuardian() {
            this.cluster = Cluster.get(getContext().getSystem());
            this.majorityMinCap = getContext().getSystem().settings().config()
                    .getInt("tapio.cluster.sharding.distributed-data.majority-min-cap");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Start.class, this::onStart)
                    .match(StartProxy.class, this::onStartProxy)
                    .build();
        }

        private void onStart(Start cmd) {
            try {
                String encName = URLEncoder.encode(cmd.typeName(), StandardCharsets.UTF_8);
                String coordinatorPath = getSelf().path()
                        .child(encName + "Coordinator")
                        .child("singleton")
                        .toStringWithoutAddress();

                Optional<ActorRef> existing = getContext().findChild(encName);
                ActorRef region;
                if (existing.isPresent()) {
                    region = existing.get();
                } else {
                    ActorRef shardReplicator = replicator(cmd.settings().role());
                    ActorRef coordinatorReplicator = replicator(cmd.settings().coordinatorSingletonSettings().role());
                    RememberEntitiesStore.Provider rememberProvider = cmd.settings().rememberEntities()
                            ? new DDataRememberEntitiesProvider(
                                    cmd.typeName(), cmd.settings(), majorityMinCap, shardReplicator)
                            : null;

                    startCoordinatorIfNeeded(
                            cmd.typeName(), encName, cmd.allocationStrategy(),
                            coordinatorReplicator, rememberProvider, cmd.settings());

                    region = getContext().actorOf(
                            ShardRegion.props(
                                    cmd.typeName(),
                                    cmd.entityPropsFactory(),
                                    cmd.settings(),
                                    coordinatorPath,
                                    cmd.messageExtractor(),
                                    cmd.handOffStopMessage(),
                                    rememberProvider),
                            encName);
                }
                getSender().tell(new Started(region), getSelf());
            } catch (Throwable e) {
                getSender().tell(new Status.Failure(e), getSelf());
            }
        }

        private void onStartProxy(StartProxy cmd) {
            try {
                String encType = URLEncoder.encode(cmd.typeName(), StandardCharsets.UTF_8);
                String proxyName = URLEncoder.encode(cmd.typeName() + "Proxy", StandardCharsets.UTF_8);
                String coordinatorPath = getSelf().path()
                        .child(encType + "Coordinator")
                        .child("singleton")
                        .toStringWithoutAddress();

                Optional<ActorRef> existing = getContext().findChild(proxyName);
                ActorRef proxy = existing.orElseGet(() -> getContext().actorOf(
                        ShardRegion.proxyProps(
                                cmd.typeName(),
                                cmd.settings(),
                                coordinatorPath,
                                cmd.messageExtractor()),
                        proxyName));
                getSender().tell(new Started(proxy), getSelf());
            } catch (Throwable e) {
                getSender().tell(new Status.Failure(e), getSelf());
            }
        }

        private void startCoordinatorIfNeeded(
                String typeName,
                String encName,
                ShardAllocationStrategy allocationStrategy,
                ActorRef coordinatorReplicator,
                RememberEntitiesStore.Provider rememberProvider,
                ClusterShardingSettings settings) {
            String coordinatorManagerName = encName + "Coordinator";
            if (getContext().findChild(coordinatorManagerName).isPresent()) {
                return;
            }
            ClusterSingletonManagerSettings singletonSettings = settings.coordinatorSingletonSettings()
                    .withSingletonName("singleton");
            if (singletonSettings.leaseUsageSettings().isEmpty()
                    && getContext().getSystem().settings().config().hasPath("tapio.cluster.sharding.use-lease")) {
                String leaseImpl = getContext().getSystem().settings().config()
                        .getString("tapio.cluster.sharding.use-lease");
                if (leaseImpl != null && !leaseImpl.isBlank()) {
                    Duration retry = getContext().getSystem().settings().config()
                            .getDuration("tapio.cluster.sharding.lease-retry-interval");
                    singletonSettings = singletonSettings.withLeaseUsageSettings(
                            new LeaseUsageSettings(leaseImpl, retry));
                }
            }
            Props coordinatorProps = ShardCoordinator.props(
                    typeName,
                    settings,
                    allocationStrategy,
                    coordinatorReplicator,
                    majorityMinCap,
                    rememberProvider);
            getContext().actorOf(
                    ClusterSingletonManager.props(
                            coordinatorProps,
                            ShardCoordinator.Internal.Terminate.INSTANCE,
                            singletonSettings),
                    coordinatorManagerName);
        }

        private ActorRef replicator(Optional<String> role) {
            ActorRef existing = replicatorByRole.get(role);
            if (existing != null && getContext().findChild(existing.path().name()).isPresent()) {
                return existing;
            }
            String name = role.filter(r -> !r.isEmpty())
                    .map(r -> URLEncoder.encode(r, StandardCharsets.UTF_8) + "Replicator")
                    .orElse("replicator");
            ActorRef ref = getContext().actorOf(Replicator.props(replicatorSettings(role)), name);
            replicatorByRole.put(role, ref);
            return ref;
        }

        private ReplicatorSettings replicatorSettings(Optional<String> role) {
            ReplicatorSettings configured = ReplicatorSettings.create(
                    getContext().getSystem().settings().config()
                            .getConfig("tapio.cluster.sharding.distributed-data"));
            Set<String> replicatorRoles = new HashSet<>();
            replicatorRoles.add(ClusterSettings.DcRolePrefix + cluster.settings().SelfDataCenter);
            role.filter(r -> !r.isEmpty()).ifPresent(replicatorRoles::add);
            return configured.withRoles(replicatorRoles);
        }
    }

    private final ExtendedActorSystem system;
    private final Cluster cluster;
    private final ActorRef guardian;
    private final Duration startTimeout = Duration.ofSeconds(10);
    private final Map<String, ActorRef> regions = new ConcurrentHashMap<>();
    private final Map<String, ActorRef> proxies = new ConcurrentHashMap<>();

    private ClusterSharding(ExtendedActorSystem system) {
        this.system = system;
        this.cluster = Cluster.get(system);
        String guardianName = system.settings().config().getString("tapio.cluster.sharding.guardian-name");
        this.guardian = system.actorOf(
                Props.create(ClusterShardingGuardian.class, ClusterShardingGuardian::new),
                guardianName);
    }

    /**
     * 使用默认分配策略和 {@link PoisonPill} 停止消息启动指定实体类型的分片区域。
     */
    public ActorRef start(
            String typeName,
            Props entityProps,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor) {
        return start(
                typeName,
                ignored -> entityProps,
                settings,
                messageExtractor,
                defaultShardAllocationStrategy(settings),
                PoisonPill.getInstance());
    }

    /**
     * 使用动态 {@code entityId -> Props} 工厂与自定义分配策略启动分片区域。
     */
    public ActorRef start(
            String typeName,
            Function<String, Props> entityPropsFactory,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor,
            ShardAllocationStrategy allocationStrategy,
            Object handOffStopMessage) {
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(entityPropsFactory, "entityPropsFactory");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(messageExtractor, "messageExtractor");
        Objects.requireNonNull(allocationStrategy, "allocationStrategy");

        ActorRef existing = regions.get(typeName);
        if (existing != null) {
            return existing;
        }
        boolean matchesRole = settings.role().map(r -> cluster.selfMember().hasRole(r)).orElse(true);
        if (!matchesRole) {
            return startProxy(typeName, settings.role(), messageExtractor);
        }
        ActorRef region = askGuardian(new Start(
                typeName, entityPropsFactory, settings, messageExtractor, allocationStrategy, handOffStopMessage));
        regions.putIfAbsent(typeName, region);
        return regions.get(typeName);
    }

    /**
     * 仅以代理模式启动指定实体类型的 {@link ShardRegion}。
     */
    public ActorRef startProxy(
            String typeName,
            Optional<String> role,
            ShardRegion.MessageExtractor messageExtractor) {
        ActorRef existing = proxies.get(typeName);
        if (existing != null) {
            return existing;
        }
        ClusterShardingSettings settings = ClusterShardingSettings.create(system)
                .withRole(role.orElse(""));
        ActorRef proxy = askGuardian(new StartProxy(typeName, settings, messageExtractor));
        proxies.putIfAbsent(typeName, proxy);
        return proxies.get(typeName);
    }

    private ActorRef askGuardian(Object cmd) {
        try {
            Object reply = Patterns.ask(guardian, cmd, startTimeout)
                    .toCompletableFuture()
                    .get(startTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (reply instanceof Started s) {
                return s.shardRegion();
            }
            if (reply instanceof Status.Failure f) {
                if (f.cause() instanceof RuntimeException re) {
                    throw re;
                }
                throw new IllegalStateException("ClusterSharding guardian failed", f.cause());
            }
            throw new IllegalStateException("Unexpected reply from ClusterShardingGuardian: " + reply);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start ClusterSharding", e);
        }
    }

    /**
     * 获取已启动的 {@link ShardRegion}（或代理）引用。
     */
    public ActorRef shardRegion(String typeName) {
        ActorRef ref = regions.get(typeName);
        if (ref != null) {
            return ref;
        }
        ActorRef proxy = proxies.get(typeName);
        if (proxy != null) {
            return proxy;
        }
        throw new IllegalArgumentException("Shard type [" + typeName + "] must be started first");
    }

    /**
     * 根据配置构建默认的 {@link ShardAllocationStrategy}。
     */
    public ShardAllocationStrategy defaultShardAllocationStrategy(ClusterShardingSettings settings) {
        var tuning = settings.tuningParameters();
        return new LeastShardAllocationStrategy(
                tuning.leastShardAllocationAbsoluteLimit(),
                tuning.leastShardAllocationRelativeLimit(),
                tuning.leastShardAllocationRebalanceThreshold(),
                tuning.leastShardAllocationMaxSimultaneousRebalance());
    }
}
