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
package io.github.espen7.tapio.cluster;

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.Address;

import java.net.MalformedURLException;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tapio Cluster 配置视图。
 */
public final class ClusterSettings {
    /** 默认数据中心名称。 */
    public static final String DefaultDataCenter = "default";
    /** 数据中心角色前缀。 */
    public static final String DcRolePrefix = "dc-";

    /** 原始配置对象。 */
    public final Config config;
    /** 种子节点地址列表。 */
    public final List<Address> SeedNodes;
    /** 单个种子节点联系超时。 */
    public final Duration SeedNodeTimeout;
    /** 重试加入失败种子节点的间隔。 */
    public final Duration RetryUnsuccessfulJoinAfter;
    /** 多次加入失败后自动关闭系统的时限（为空表示关闭）。 */
    public final Optional<Duration> ShutdownAfterUnsuccessfulJoinSeedNodes;
    /** 节点角色集合（含 {@code dc-<dataCenter>}）。 */
    public final Set<String> Roles;
    /** 所属数据中心名称。 */
    public final String SelfDataCenter;
    /** 应用版本号。 */
    public final String AppVersion;
    /** 集群达到最小成员数后才将 Joining 升为 Up。 */
    public final int MinNrOfMembers;
    /** 按角色要求的最小成员数映射。 */
    public final Map<String, Integer> MinNrOfMembersOfRole;
    /** Gossip 发送间隔。 */
    public final Duration GossipInterval;
    /** Gossip TTL。 */
    public final Duration GossipTimeToLive;
    /** Leader 周期性动作间隔。 */
    public final Duration LeaderActionsInterval;
    /** 不可达节点扫描间隔。 */
    public final Duration UnreachableNodesReaperInterval;
    /** 集群内部 Actor 使用的 Dispatcher id。 */
    public final String UseDispatcher;
    /** 允许 WeaklyUp 的等待时长（为空表示禁用）。 */
    public final Optional<Duration> AllowWeaklyUpAfter;
    /** 清理 Gossip tombstone 的时长。 */
    public final Duration PruneGossipTombstonesAfter;
    /** Down 后延迟移除的边际时长。 */
    public final Duration DownRemovalMargin;
    /** 移除节点后隔离时长。 */
    public final Duration QuarantineRemovedNodeAfter;
    /** 节点被 Down 后是否触发 CoordinatedShutdown。 */
    public final boolean RunCoordinatedShutdownWhenDown;
    /** DowningProvider 实现类名。 */
    public final String DowningProviderClassName;
    /** 失效检测器配置节。 */
    public final Config FailureDetectorConfig;
    /** 失效检测器实现类名。 */
    public final String FailureDetectorImplementationClass;
    /** 心跳发送间隔。 */
    public final Duration HeartbeatInterval;
    /** 期望收到心跳响应的等待时长。 */
    public final Duration HeartbeatExpectedResponseAfter;
    /** 每个节点监控的邻居数量。 */
    public final int MonitoredByNrOfMembers;

    /**
     * 从系统配置构造 ClusterSettings。
     *
     * @param config 系统配置
     * @param systemName ActorSystem 名称
     */
    public ClusterSettings(Config config, String systemName) {
        this.config = config;
        Config cc = config.getConfig("tapio.cluster");
        this.SeedNodes = cc.getStringList("seed-nodes").stream()
                .map(s -> {
                    try {
                        return ActorPath.fromString(s).address();
                    } catch (MalformedURLException e) {
                        throw new ConfigurationException("Invalid seed-node URI [" + s + "]", e);
                    }
                })
                .toList();
        this.SeedNodeTimeout = cc.getDuration("seed-node-timeout");
        this.RetryUnsuccessfulJoinAfter = cc.getDuration("retry-unsuccessful-join-after");
        this.ShutdownAfterUnsuccessfulJoinSeedNodes = parseOptionalDuration(
                cc, "shutdown-after-unsuccessful-join-seed-nodes");
        this.SelfDataCenter = cc.getString("multi-data-center.self-data-center");
        Set<String> rolesSet = new HashSet<>(cc.getStringList("roles"));
        rolesSet.add(DcRolePrefix + this.SelfDataCenter);
        this.Roles = Set.copyOf(rolesSet);
        this.AppVersion = cc.getString("app-version");
        this.MinNrOfMembers = cc.getInt("min-nr-of-members");
        Map<String, Integer> roleMins = new HashMap<>();
        Config roleCfg = cc.getConfig("role");
        for (String r : roleCfg.root().keySet()) {
            if (roleCfg.hasPath(r + ".min-nr-of-members")) {
                roleMins.put(r, roleCfg.getInt(r + ".min-nr-of-members"));
            }
        }
        this.MinNrOfMembersOfRole = Map.copyOf(roleMins);
        this.GossipInterval = cc.getDuration("gossip-interval");
        this.GossipTimeToLive = cc.getDuration("gossip-time-to-live");
        this.LeaderActionsInterval = cc.getDuration("leader-actions-interval");
        this.UnreachableNodesReaperInterval = cc.getDuration("unreachable-nodes-reaper-interval");
        this.UseDispatcher = cc.getString("use-dispatcher");
        this.AllowWeaklyUpAfter = parseOptionalDuration(cc, "allow-weakly-up-members");
        this.PruneGossipTombstonesAfter = cc.getDuration("prune-gossip-tombstones-after");
        this.DownRemovalMargin = parseOptionalDuration(cc, "down-removal-margin").orElse(Duration.ZERO);
        this.QuarantineRemovedNodeAfter = cc.getDuration("quarantine-removed-node-after");
        this.RunCoordinatedShutdownWhenDown = cc.getBoolean("run-coordinated-shutdown-when-down");
        String dp = cc.getString("downing-provider-class");
        this.DowningProviderClassName = dp.isEmpty() ? NoDowning.class.getName() : dp;
        this.FailureDetectorConfig = cc.getConfig("failure-detector");
        this.FailureDetectorImplementationClass = FailureDetectorConfig.getString("implementation-class");
        this.HeartbeatInterval = FailureDetectorConfig.getDuration("heartbeat-interval");
        this.HeartbeatExpectedResponseAfter = FailureDetectorConfig.getDuration("expected-response-after");
        this.MonitoredByNrOfMembers = FailureDetectorConfig.getInt("monitored-by-nr-of-members");
    }

    private static Optional<Duration> parseOptionalDuration(Config cfg, String path) {
        String raw = cfg.getString(path).toLowerCase(Locale.ROOT);
        if ("off".equals(raw) || "false".equals(raw)) {
            return Optional.empty();
        }
        return Optional.of(cfg.getDuration(path));
    }
}
