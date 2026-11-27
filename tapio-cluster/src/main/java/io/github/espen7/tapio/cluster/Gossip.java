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

import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * INTERNAL API：集群成员环状态快照（受 {@link VectorClock} 版本控制）。
 */
@InternalApi
public final class Gossip implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 空 Gossip 状态。 */
    public static final Gossip EMPTY = new Gossip(
            Collections.unmodifiableNavigableSet(new TreeSet<>(Member.ORDERING)),
            GossipOverview.EMPTY,
            VectorClock.EMPTY,
            Map.of());

    private final NavigableSet<Member> members;
    private final GossipOverview overview;
    private final VectorClock version;
    private final Map<UniqueAddress, Long> tombstones;
    private transient volatile Map<UniqueAddress, Member> membersMapCache;

    /**
     * 构造不可变 {@link Gossip} 状态。
     *
     * @param members 有序成员集合
     * @param overview Gossip 概览
     * @param version 向量时钟版本
     * @param tombstones 已移除成员墓碑时间戳映射
     */
    public Gossip(
            NavigableSet<Member> members,
            GossipOverview overview,
            VectorClock version,
            Map<UniqueAddress, Long> tombstones) {
        TreeSet<Member> sorted = new TreeSet<>(Member.ORDERING);
        sorted.addAll(Objects.requireNonNull(members, "members"));
        this.members = Collections.unmodifiableNavigableSet(sorted);
        this.overview = Objects.requireNonNull(overview, "overview");
        this.version = Objects.requireNonNull(version, "version");
        this.tombstones = Map.copyOf(Objects.requireNonNull(tombstones, "tombstones"));
    }

    /**
     * 计算节点对应的向量时钟键名。
     *
     * @param node 节点唯一地址
     * @return 向量时钟哈希键
     */
    public static String vclockName(UniqueAddress node) {
        return VectorClock.node(node.address() + "-" + node.longUid());
    }

    public NavigableSet<Member> members() {
        return members;
    }

    public GossipOverview overview() {
        return overview;
    }

    public VectorClock version() {
        return version;
    }

    public Map<UniqueAddress, Long> tombstones() {
        return tombstones;
    }

    private Map<UniqueAddress, Member> membersMap() {
        Map<UniqueAddress, Member> map = membersMapCache;
        if (map == null) {
            Map<UniqueAddress, Member> built = new HashMap<>();
            for (Member m : members) {
                built.put(m.uniqueAddress(), m);
            }
            map = Collections.unmodifiableMap(built);
            membersMapCache = map;
        }
        return map;
    }

    /** 判断集群中是否包含指定节点。 */
    public boolean hasMember(UniqueAddress node) {
        return membersMap().containsKey(node);
    }

    /** 查找指定节点的 {@link Member}；不存在时返回 {@link Member#removed(UniqueAddress)}。 */
    public Member member(UniqueAddress node) {
        return membersMap().getOrDefault(node, Member.removed(node));
    }

    /** 使用当前节点的向量时钟键递增版本，并重置 {@code seen} 为仅包含当前节点。 */
    public Gossip increment(String nodeHash, UniqueAddress selfUniqueAddress) {
        VectorClock nextClock = version.increment(nodeHash);
        GossipOverview nextOverview = new GossipOverview(Set.of(selfUniqueAddress), overview.reachability());
        return new Gossip(members, nextOverview, nextClock, tombstones);
    }

    /** 标记指定节点已见过当前 Gossip 版本。 */
    public Gossip seen(UniqueAddress node) {
        if (overview.seen().contains(node) || !hasMember(node)) {
            return this;
        }
        return new Gossip(members, overview.seen(node), version, tombstones);
    }

    /** 清除 {@code seen} 集合。 */
    public Gossip clearSeen() {
        if (overview.seen().isEmpty()) {
            return this;
        }
        return new Gossip(members, new GossipOverview(Set.of(), overview.reachability()), version, tombstones);
    }

    /** 返回仅含新成员的 Gossip 更新副本。 */
    public Gossip withMembers(NavigableSet<Member> nextMembers) {
        return new Gossip(nextMembers, overview, version, tombstones);
    }

    /** 返回更新了可达性表的 Gossip 副本。 */
    public Gossip withReachability(Reachability reachability) {
        return new Gossip(members, new GossipOverview(overview.seen(), reachability), version, tombstones);
    }

    /** 返回添加了墓碑记录的 Gossip 副本。 */
    public Gossip withTombstones(Map<UniqueAddress, Long> nextTombstones) {
        return new Gossip(members, overview, version, nextTombstones);
    }

    /**
     * 合并两份冲突或并发的 Gossip 状态。
     *
     * @param that 远端 Gossip
     * @return 合并后的 Gossip
     */
    public Gossip merge(Gossip that) {
        VectorClock.Ordering cmp = this.version.compareTo(that.version);
        if (cmp == VectorClock.Ordering.After) {
            return this;
        }
        if (cmp == VectorClock.Ordering.Before) {
            return that;
        }
        if (cmp == VectorClock.Ordering.Same) {
            Set<UniqueAddress> mergedSeen = new HashSet<>(this.overview.seen());
            mergedSeen.addAll(that.overview.seen());
            Set<UniqueAddress> memberUas = membersMap().keySet();
            mergedSeen.retainAll(memberUas);
            Reachability mergedReach = this.overview.reachability().merge(memberUas, that.overview.reachability());
            return new Gossip(this.members, new GossipOverview(mergedSeen, mergedReach), this.version, this.tombstones);
        }

        Map<UniqueAddress, Long> mergedTombstones = new HashMap<>(this.tombstones);
        for (Map.Entry<UniqueAddress, Long> e : that.tombstones.entrySet()) {
            mergedTombstones.merge(e.getKey(), e.getValue(), Math::max);
        }

        Map<UniqueAddress, Member> mergedMembersMap = new HashMap<>();
        for (Member m : this.members) {
            if (!mergedTombstones.containsKey(m.uniqueAddress())) {
                mergedMembersMap.put(m.uniqueAddress(), m);
            }
        }
        for (Member m : that.members) {
            UniqueAddress ua = m.uniqueAddress();
            if (mergedTombstones.containsKey(ua)) {
                continue;
            }
            Member existing = mergedMembersMap.get(ua);
            if (existing == null) {
                mergedMembersMap.put(ua, m);
            } else {
                mergedMembersMap.put(ua, mergeMemberStatus(existing, m));
            }
        }

        TreeSet<Member> mergedMembers = new TreeSet<>(Member.ORDERING);
        mergedMembers.addAll(mergedMembersMap.values());
        Set<UniqueAddress> allowedUas = mergedMembersMap.keySet();

        Reachability mergedReach = this.overview.reachability().merge(allowedUas, that.overview.reachability());
        VectorClock mergedVersion = this.version.merge(that.version);
        return new Gossip(
                mergedMembers,
                new GossipOverview(Set.of(), mergedReach),
                mergedVersion,
                mergedTombstones);
    }

    private static Member mergeMemberStatus(Member a, Member b) {
        if (a.status() == b.status()) {
            return a.upNumber() <= b.upNumber() ? a : b;
        }
        if (a.status() == MemberStatus.Down) return a;
        if (b.status() == MemberStatus.Down) return b;
        if (a.status() == MemberStatus.Removed) return a;
        if (b.status() == MemberStatus.Removed) return b;
        if (a.status() == MemberStatus.Exiting) return a;
        if (b.status() == MemberStatus.Exiting) return b;
        if (a.status() == MemberStatus.Leaving) return a;
        if (b.status() == MemberStatus.Leaving) return b;
        if (a.status() == MemberStatus.Up) return a;
        if (b.status() == MemberStatus.Up) return b;
        if (a.status() == MemberStatus.WeaklyUp) return a;
        if (b.status() == MemberStatus.WeaklyUp) return b;
        return a;
    }

    /**
     * 判断当前数据中心是否已达到 Gossip 收敛（所有可达非 Down 成员均已见过当前版本）。
     *
     * @param selfUniqueAddress 本节点唯一地址
     * @return 已收敛返回 {@code true}
     */
    public boolean convergence(UniqueAddress selfUniqueAddress) {
        Set<UniqueAddress> unreachable = overview.reachability().allUnreachableOrTerminated();
        for (Member m : members) {
            if (m.status() == MemberStatus.Down) {
                continue;
            }
            if (unreachable.contains(m.uniqueAddress())) {
                return false;
            }
            if (!overview.seen().contains(m.uniqueAddress())) {
                return false;
            }
        }
        return !members.isEmpty();
    }

    /**
     * 返回指定数据中心的 Leader 唯一地址（按 {@link Member#LEADER_STATUS_ORDERING} 选举）。
     *
     * @param dataCenter 数据中心名称
     * @return Leader 唯一地址（若无活跃成员则为空）
     */
    public Optional<UniqueAddress> leaderOf(String dataCenter) {
        Set<UniqueAddress> unreachable = overview.reachability().allUnreachableOrTerminated();
        return members.stream()
                .filter(m -> m.dataCenter().equals(dataCenter))
                .filter(m -> m.status() != MemberStatus.Down && !unreachable.contains(m.uniqueAddress()))
                .min(Member.LEADER_STATUS_ORDERING)
                .map(Member::uniqueAddress);
    }

    /**
     * 返回指定角色在数据中心内的 Leader 唯一地址。
     *
     * @param role 角色名称
     * @param dataCenter 数据中心名称
     * @return 角色 Leader 唯一地址
     */
    public Optional<UniqueAddress> roleLeaderOf(String role, String dataCenter) {
        Set<UniqueAddress> unreachable = overview.reachability().allUnreachableOrTerminated();
        return members.stream()
                .filter(m -> m.dataCenter().equals(dataCenter) && m.hasRole(role))
                .filter(m -> m.status() != MemberStatus.Down && !unreachable.contains(m.uniqueAddress()))
                .min(Member.LEADER_STATUS_ORDERING)
                .map(Member::uniqueAddress);
    }

    /** 返回集群中所有角色集合。 */
    public Set<String> allRoles() {
        Set<String> roles = new HashSet<>();
        for (Member m : members) {
            roles.addAll(m.roles());
        }
        return Collections.unmodifiableSet(roles);
    }

    /** 判断本节点是否为当前数据中心的 Leader。 */
    public boolean isLeader(UniqueAddress selfUniqueAddress, String selfDataCenter) {
        return leaderOf(selfDataCenter).map(selfUniqueAddress::equals).orElse(false);
    }
}
