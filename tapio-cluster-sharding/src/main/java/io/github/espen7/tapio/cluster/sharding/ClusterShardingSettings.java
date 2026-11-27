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

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.singleton.ClusterSingletonManagerSettings;
import io.github.espen7.tapio.coordination.lease.LeaseUsageSettings;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link ClusterSharding} 扩展与 {@link ShardRegion}/{@link ShardCoordinator} 的配置项。
 */
public final class ClusterShardingSettings implements NoSerializationVerificationNeeded {

    /** 持久化模式常量（Tapio 当前不支持，仅保留常量用于显式校验拒绝）。 */
    public static final String StateStoreModePersistence = "persistence";
    /** 分布式数据（DData）状态存储模式常量。 */
    public static final String StateStoreModeDData = "ddata";

    /** INTERNAL API：DData 记住实体存储常量。 */
    @InternalApi
    public static final String RememberEntitiesStoreDData = "ddata";
    /** INTERNAL API：EventSourced 记住实体存储常量（Tapio 当前不支持）。 */
    @InternalApi
    public static final String RememberEntitiesStoreEventsourced = "eventsourced";

    /**
     * 分片调优参数。
     */
    public record TuningParameters(
            Duration coordinatorFailureBackoff,
            Duration retryInterval,
            int bufferSize,
            Duration handOffTimeout,
            Duration shardStartTimeout,
            Duration shardFailureBackoff,
            Duration entityRestartBackoff,
            Duration rebalanceInterval,
            int leastShardAllocationRebalanceThreshold,
            int leastShardAllocationMaxSimultaneousRebalance,
            Duration waitingForStateTimeout,
            Duration updatingStateTimeout,
            int coordinatorStateWriteMajorityPlus,
            int coordinatorStateReadMajorityPlus,
            int leastShardAllocationAbsoluteLimit,
            double leastShardAllocationRelativeLimit) {
    }

    private final Optional<String> role;
    private final boolean rememberEntities;
    private final String stateStoreMode;
    private final String rememberEntitiesStore;
    private final Duration passivateIdleEntityAfter;
    private final TuningParameters tuningParameters;
    private final ClusterSingletonManagerSettings coordinatorSingletonSettings;

    /**
     * 构造 {@link ClusterShardingSettings}。
     */
    public ClusterShardingSettings(
            Optional<String> role,
            boolean rememberEntities,
            String stateStoreMode,
            String rememberEntitiesStore,
            Duration passivateIdleEntityAfter,
            TuningParameters tuningParameters,
            ClusterSingletonManagerSettings coordinatorSingletonSettings) {
        this.role = Objects.requireNonNull(role, "role");
        this.rememberEntities = rememberEntities;
        this.stateStoreMode = normalizeAndValidateStateStoreMode(stateStoreMode);
        this.rememberEntitiesStore = normalizeAndValidateRememberEntitiesStore(rememberEntitiesStore);
        this.passivateIdleEntityAfter = Objects.requireNonNull(passivateIdleEntityAfter, "passivateIdleEntityAfter");
        this.tuningParameters = Objects.requireNonNull(tuningParameters, "tuningParameters");
        this.coordinatorSingletonSettings = Objects.requireNonNull(
                coordinatorSingletonSettings, "coordinatorSingletonSettings");
    }

    private static String normalizeAndValidateStateStoreMode(String mode) {
        String lower = Objects.requireNonNull(mode, "stateStoreMode").toLowerCase(Locale.ROOT);
        if (StateStoreModePersistence.equals(lower)) {
            throw new IllegalArgumentException(
                    "state-store-mode = \"persistence\" is not supported in Tapio (tapio-persistence is excluded); use \"ddata\".");
        }
        if (!StateStoreModeDData.equals(lower)) {
            throw new IllegalArgumentException("Unknown state-store-mode [" + mode + "], expected \"ddata\".");
        }
        return lower;
    }

    private static String normalizeAndValidateRememberEntitiesStore(String store) {
        String lower = Objects.requireNonNull(store, "rememberEntitiesStore").toLowerCase(Locale.ROOT);
        if (RememberEntitiesStoreEventsourced.equals(lower)) {
            throw new IllegalArgumentException(
                    "remember-entities-store = \"eventsourced\" is not supported in Tapio; use \"ddata\".");
        }
        if (!RememberEntitiesStoreDData.equals(lower)) {
            throw new IllegalArgumentException("Unknown remember-entities-store [" + store + "], expected \"ddata\".");
        }
        return lower;
    }

    /**
     * 从 {@link ActorSystem} 默认配置 {@code tapio.cluster.sharding} 创建设置。
     *
     * @param system ActorSystem
     * @return {@link ClusterShardingSettings}
     */
    public static ClusterShardingSettings create(ActorSystem system) {
        Config shardingConfig = system.settings().config().getConfig("tapio.cluster.sharding");
        Config coordinatorSingletonConfig = shardingConfig.getConfig("coordinator-singleton");
        return create(shardingConfig, ClusterSingletonManagerSettings.create(coordinatorSingletonConfig));
    }

    /**
     * 从指定配置对象创建设置。
     *
     * @param config 分片配置节
     * @param coordinatorSingletonSettings 协调器单例设置
     * @return {@link ClusterShardingSettings}
     */
    public static ClusterShardingSettings create(
            Config config,
            ClusterSingletonManagerSettings coordinatorSingletonSettings) {
        String roleStr = config.getString("role");
        Optional<String> role = roleStr.isEmpty() ? Optional.empty() : Optional.of(roleStr);
        boolean rememberEntities = config.getBoolean("remember-entities");
        String stateStoreMode = config.getString("state-store-mode");
        String rememberEntitiesStore = config.getString("remember-entities-store");
        Duration passivateIdleAfter = parsePassivateIdle(config);

        int writeMajorityPlus = parseMajorityPlus(config, "coordinator-state.write-majority-plus");
        int readMajorityPlus = parseMajorityPlus(config, "coordinator-state.read-majority-plus");

        TuningParameters tuning = new TuningParameters(
                config.getDuration("coordinator-failure-backoff"),
                config.getDuration("retry-interval"),
                config.getInt("buffer-size"),
                config.getDuration("handoff-timeout"),
                config.getDuration("shard-start-timeout"),
                config.getDuration("shard-failure-backoff"),
                config.getDuration("entity-restart-backoff"),
                config.getDuration("rebalance-interval"),
                config.getInt("least-shard-allocation-strategy.rebalance-threshold"),
                config.getInt("least-shard-allocation-strategy.max-simultaneous-rebalance"),
                config.getDuration("waiting-for-state-timeout"),
                config.getDuration("updating-state-timeout"),
                writeMajorityPlus,
                readMajorityPlus,
                config.getInt("least-shard-allocation-strategy.rebalance-absolute-limit"),
                config.getDouble("least-shard-allocation-strategy.rebalance-relative-limit"));

        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleAfter,
                tuning,
                coordinatorSingletonSettings.withRole(role));
    }

    private static Duration parsePassivateIdle(Config config) {
        String raw = config.getString("passivate-idle-entity-after").trim().toLowerCase(Locale.ROOT);
        if ("off".equals(raw) || "false".equals(raw)) {
            return Duration.ZERO;
        }
        return config.getDuration("passivate-idle-entity-after");
    }

    private static int parseMajorityPlus(Config config, String path) {
        String raw = config.getString(path).trim().toLowerCase(Locale.ROOT);
        if ("all".equals(raw)) {
            return Integer.MAX_VALUE;
        }
        return config.getInt(path);
    }

    public Optional<String> role() {
        return role;
    }

    public boolean rememberEntities() {
        return rememberEntities;
    }

    public String stateStoreMode() {
        return stateStoreMode;
    }

    public String rememberEntitiesStore() {
        return rememberEntitiesStore;
    }

    public Duration passivateIdleEntityAfter() {
        return passivateIdleEntityAfter;
    }

    public TuningParameters tuningParameters() {
        return tuningParameters;
    }

    public ClusterSingletonManagerSettings coordinatorSingletonSettings() {
        return coordinatorSingletonSettings;
    }

    public ClusterShardingSettings withRole(String role) {
        Optional<String> r = (role == null || role.isEmpty()) ? Optional.empty() : Optional.of(role);
        return new ClusterShardingSettings(
                r,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleEntityAfter,
                tuningParameters,
                coordinatorSingletonSettings.withRole(r));
    }

    public ClusterShardingSettings withRememberEntities(boolean rememberEntities) {
        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleEntityAfter,
                tuningParameters,
                coordinatorSingletonSettings);
    }

    public ClusterShardingSettings withPassivateIdleEntityAfter(Duration duration) {
        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                duration,
                tuningParameters,
                coordinatorSingletonSettings);
    }

    public ClusterShardingSettings withStateStoreMode(String stateStoreMode) {
        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleEntityAfter,
                tuningParameters,
                coordinatorSingletonSettings);
    }

    public Optional<LeaseUsageSettings> leaseUsageSettings() {
        return coordinatorSingletonSettings.leaseUsageSettings();
    }

    public ClusterShardingSettings withLeaseUsageSettings(LeaseUsageSettings leaseUsageSettings) {
        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleEntityAfter,
                tuningParameters,
                coordinatorSingletonSettings.withLeaseUsageSettings(leaseUsageSettings));
    }

    public ClusterShardingSettings withTuningParameters(TuningParameters tuningParameters) {
        return new ClusterShardingSettings(
                role,
                rememberEntities,
                stateStoreMode,
                rememberEntitiesStore,
                passivateIdleEntityAfter,
                tuningParameters,
                coordinatorSingletonSettings);
    }
}
