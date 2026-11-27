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
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.PoisonPill;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterSettings;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.ReplicatorSettings;
import io.github.espen7.tapio.cluster.sharding.internal.DDataRememberEntitiesProvider;
import io.github.espen7.tapio.cluster.sharding.internal.LeastShardAllocationStrategy;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManager;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManagerSettings;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 集群分片扩展入口（基于 DData 模式），负责在集群节点上启动 {@link ShardRegion} 与 {@link ShardCoordinator} 单例。
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
     *
     * @param system ActorSystem
     * @return ClusterSharding 扩展
     */
    public static ClusterSharding get(ActorSystem system) {
        return Id.apply(system);
    }

    private final ExtendedActorSystem system;
    private final Cluster cluster;
    private final String guardianName;
    private final int majorityMinCap;
    private final Map<String, ActorRef> regions = new ConcurrentHashMap<>();
    private final Map<String, ActorRef> proxies = new ConcurrentHashMap<>();
    private final Map<Optional<String>, ActorRef> replicatorByRole = new ConcurrentHashMap<>();

    private ClusterSharding(ExtendedActorSystem system) {
        this.system = system;
        this.cluster = Cluster.get(system);
        var shardingConfig = system.settings().config().getConfig("tapio.cluster.sharding");
        this.guardianName = shardingConfig.getString("guardian-name");
        this.majorityMinCap = shardingConfig.getInt("distributed-data.majority-min-cap");
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

        String encodedType = URLEncoder.encode(typeName, StandardCharsets.UTF_8);
        String coordinatorName = encodedType + "Coordinator";
        String coordinatorSingletonPath = "/user/" + guardianName + "-" + coordinatorName;

        ActorRef shardReplicator = replicator(settings.role());
        ActorRef coordinatorReplicator = replicator(settings.coordinatorSingletonSettings().role());
        RememberEntitiesStore.Provider rememberProvider = settings.rememberEntities()
                ? new DDataRememberEntitiesProvider(typeName, settings, majorityMinCap, shardReplicator)
                : null;

        ClusterSingletonManagerSettings singletonSettings = settings.coordinatorSingletonSettings()
                .withSingletonName("singleton");
        if (settings.coordinatorSingletonSettings().leaseUsageSettings().isEmpty()
                && system.settings().config().hasPath("tapio.cluster.sharding.use-lease")) {
            String leaseImpl = system.settings().config().getString("tapio.cluster.sharding.use-lease");
            if (leaseImpl != null && !leaseImpl.isBlank()) {
                java.time.Duration retry = system.settings().config()
                        .getDuration("tapio.cluster.sharding.lease-retry-interval");
                singletonSettings = singletonSettings.withLeaseUsageSettings(
                        new io.github.espen7.tapio.coordination.lease.LeaseUsageSettings(leaseImpl, retry));
            }
        }
        Props coordinatorProps = ShardCoordinator.props(
                typeName,
                settings,
                allocationStrategy,
                coordinatorReplicator,
                majorityMinCap,
                rememberProvider);

        system.actorOf(
                ClusterSingletonManager.props(
                        coordinatorProps,
                        ShardCoordinator.Internal.Terminate.INSTANCE,
                        singletonSettings),
                guardianName + "-" + coordinatorName);

        String coordinatorPath = coordinatorSingletonPath + "/singleton";
        ActorRef region = system.actorOf(
                ShardRegion.props(
                        typeName,
                        entityPropsFactory,
                        settings,
                        coordinatorPath,
                        messageExtractor,
                        handOffStopMessage,
                        rememberProvider),
                guardianName + "-" + encodedType);
        regions.put(typeName, region);
        return region;
    }

    /**
     * 仅以代理模式启动指定实体类型的 {@link ShardRegion}。
     */
    public ActorRef startProxy(
            String typeName,
            java.util.Optional<String> role,
            ShardRegion.MessageExtractor messageExtractor) {
        ActorRef existing = proxies.get(typeName);
        if (existing != null) {
            return existing;
        }
        ClusterShardingSettings settings = ClusterShardingSettings.create(system)
                .withRole(role.orElse(""));
        String encodedType = URLEncoder.encode(typeName, StandardCharsets.UTF_8);
        String coordinatorPath = "/user/" + guardianName + "-" + encodedType + "Coordinator/singleton";
        ActorRef proxy = system.actorOf(
                ShardRegion.proxyProps(typeName, settings, coordinatorPath, messageExtractor),
                guardianName + "-" + encodedType + "Proxy");
        proxies.put(typeName, proxy);
        return proxy;
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

    private ActorRef replicator(Optional<String> role) {
        return replicatorByRole.computeIfAbsent(role, r -> {
            String name = r.map(s -> URLEncoder.encode(s, StandardCharsets.UTF_8) + "Replicator")
                    .orElse("replicator");
            return system.actorOf(Replicator.props(replicatorSettings(r)), guardianName + "-" + name);
        });
    }

    private ReplicatorSettings replicatorSettings(Optional<String> role) {
        ReplicatorSettings configured = ReplicatorSettings.create(
                system.settings().config().getConfig("tapio.cluster.sharding.distributed-data"));
        Set<String> replicatorRoles = new HashSet<>();
        replicatorRoles.add(ClusterSettings.DcRolePrefix + cluster.settings().SelfDataCenter);
        role.filter(r -> !r.isEmpty()).ifPresent(replicatorRoles::add);
        return configured.withRoles(replicatorRoles);
    }
}
