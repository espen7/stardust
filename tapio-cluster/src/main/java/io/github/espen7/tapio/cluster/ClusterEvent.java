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

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.annotation.DoNotInherit;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * 集群领域事件体系，供 {@link Cluster#subscribe} 订阅。
 */
public final class ClusterEvent {
    private ClusterEvent() {
    }

    /** 订阅初始状态推送模式。 */
    public enum SubscriptionInitialStateMode {
        /** 先发送一次 {@link CurrentClusterState} 快照。 */
        InitialStateAsSnapshot,
        /** 将当前集群状态合成为等价的历史领域事件序列发送。 */
        InitialStateAsEvents
    }

    /** Java API：快照初始状态模式。 */
    public static SubscriptionInitialStateMode initialStateAsSnapshot() {
        return SubscriptionInitialStateMode.InitialStateAsSnapshot;
    }

    /** Java API：事件回放初始状态模式。 */
    public static SubscriptionInitialStateMode initialStateAsEvents() {
        return SubscriptionInitialStateMode.InitialStateAsEvents;
    }

    /**
     * 集群领域事件标记接口。
     */
    @DoNotInherit
    public interface ClusterDomainEvent extends DeadLetterSuppression {
    }

    /**
     * 当前集群状态快照。
     *
     * @param members 有序成员集合
     * @param unreachable 不可达成员集合
     * @param seenBy 已见过当前 Gossip 的节点地址集合
     * @param leader 当前数据中心 Leader 地址（可能为空）
     * @param roleLeaderMap 角色到 Leader 地址的映射
     */
    public record CurrentClusterState(
            NavigableSet<Member> members,
            Set<Member> unreachable,
            Set<Address> seenBy,
            Optional<Address> leader,
            Map<String, Optional<Address>> roleLeaderMap) implements Serializable {
        @Serial
        private static final long serialVersionUID = 2L;

        public static final CurrentClusterState EMPTY = new CurrentClusterState(
                Collections.unmodifiableNavigableSet(new TreeSet<>(Member.ORDERING)),
                Set.of(),
                Set.of(),
                Optional.empty(),
                Map.of());

        public CurrentClusterState {
            TreeSet<Member> sorted = new TreeSet<>(Member.ORDERING);
            sorted.addAll(Objects.requireNonNull(members, "members"));
            members = Collections.unmodifiableNavigableSet(sorted);
            unreachable = Set.copyOf(Objects.requireNonNull(unreachable, "unreachable"));
            seenBy = Set.copyOf(Objects.requireNonNull(seenBy, "seenBy"));
            leader = Objects.requireNonNull(leader, "leader");
            roleLeaderMap = Map.copyOf(Objects.requireNonNull(roleLeaderMap, "roleLeaderMap"));
        }

        /** Java API：获取成员列表。 */
        public Iterable<Member> getMembers() {
            return members;
        }

        /** Java API：获取不可达成员集合。 */
        public Set<Member> getUnreachable() {
            return unreachable;
        }

        /** Java API：获取已见地址集合。 */
        public Set<Address> getSeenBy() {
            return seenBy;
        }

        /** Java API：获取 Leader 地址（无时返回 {@code null}）。 */
        public Address getLeader() {
            return leader.orElse(null);
        }

        /** Java API：获取指定角色的 Leader 地址（无时返回 {@code null}）。 */
        public Address getRoleLeader(String role) {
            return roleLeaderMap.getOrDefault(role, Optional.empty()).orElse(null);
        }
    }

    /** 成员状态变更事件基接口。 */
    @DoNotInherit
    public interface MemberEvent extends ClusterDomainEvent {
        /** 返回发生状态变更的成员。 */
        Member member();
    }

    /** 成员加入中事件。 */
    public record MemberJoined(Member member) implements MemberEvent {
    }

    /** 成员弱可达就绪事件。 */
    public record MemberWeaklyUp(Member member) implements MemberEvent {
    }

    /** 成员上线（Up）事件。 */
    public record MemberUp(Member member) implements MemberEvent {
    }

    /** 成员开始离开事件。 */
    public record MemberLeft(Member member) implements MemberEvent {
    }

    /** 成员即将退出事件。 */
    public record MemberExited(Member member) implements MemberEvent {
    }

    /** 成员被标记为 Down 事件。 */
    public record MemberDowned(Member member) implements MemberEvent {
    }

    /**
     * 成员已被移除事件。
     *
     * @param member 被移除的成员
     * @param previousStatus 移除前的状态
     */
    public record MemberRemoved(Member member, MemberStatus previousStatus) implements MemberEvent {
    }

    /** 成员准备关闭事件。 */
    public record MemberPreparingForShutdown(Member member) implements MemberEvent {
    }

    /** 成员已就绪关闭事件。 */
    public record MemberReadyForShutdown(Member member) implements MemberEvent {
    }

    /** 可达性变更事件基接口。 */
    @DoNotInherit
    public interface ReachabilityEvent extends ClusterDomainEvent {
        /** 返回可达性发生变化的成员。 */
        Member member();
    }

    /** 成员变为不可达事件。 */
    public record UnreachableMember(Member member) implements ReachabilityEvent {
    }

    /** 成员恢复可达事件。 */
    public record ReachableMember(Member member) implements ReachabilityEvent {
    }

    /**
     * 数据中心 Leader 变更事件。
     *
     * @param leader 新 Leader 地址（无 Leader 时为空）
     */
    public record LeaderChanged(Optional<Address> leader) implements ClusterDomainEvent {
        /** Java API：返回 Leader 地址（无时为 {@code null}）。 */
        public Address getLeader() {
            return leader.orElse(null);
        }
    }

    /**
     * 角色 Leader 变更事件。
     *
     * @param role 角色名
     * @param leader 新角色 Leader 地址
     */
    public record RoleLeaderChanged(String role, Optional<Address> leader) implements ClusterDomainEvent {
        /** Java API：返回角色 Leader 地址（无时为 {@code null}）。 */
        public Address getLeader() {
            return leader.orElse(null);
        }
    }

    /** 本节点正在关闭集群连接的通知事件。 */
    public static final class ClusterShuttingDown implements ClusterDomainEvent {
        /** 单例实例。 */
        public static final ClusterShuttingDown INSTANCE = new ClusterShuttingDown();

        private ClusterShuttingDown() {
        }

        @Override
        public String toString() {
            return "ClusterShuttingDown";
        }
    }
}
