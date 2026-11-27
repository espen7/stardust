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
package io.github.espen7.tapio.cluster.sharding.internal;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * INTERNAL API：集群分片分配策略的公共混入契约与排序规则。
 */
@InternalApi
public interface ClusterShardAllocationMixin {

    /** 正在加入集群的成员状态集合。 */
    Set<MemberStatus> JOINING_CLUSTER = Set.of(MemberStatus.Joining, MemberStatus.WeaklyUp);

    /** 正在离开集群的成员状态集合。 */
    Set<MemberStatus> LEAVING_CLUSTER_STATUSES = Set.of(MemberStatus.Leaving, MemberStatus.Exiting, MemberStatus.Down);

    /** Region 与其对应集群成员及已分配分片列表的条目。 */
    record RegionEntry(ActorRef region, Member member, List<String> shardIds) {
    }

    /** 分片分配适用性排序器：优先非离开状态，其次按已分配分片数升序。 */
    Comparator<RegionEntry> SHARD_SUITABILITY_ORDERING = (x, y) -> {
        if (x.member().status() != y.member().status()) {
            boolean xIsLeaving = LEAVING_CLUSTER_STATUSES.contains(x.member().status());
            boolean yIsLeaving = LEAVING_CLUSTER_STATUSES.contains(y.member().status());
            int cmp = Boolean.compare(xIsLeaving, yIsLeaving);
            if (cmp != 0) {
                return cmp;
            }
        }
        int sizeCmp = Integer.compare(x.shardIds().size(), y.shardIds().size());
        if (sizeCmp != 0) {
            return sizeCmp;
        }
        return x.region().path().toString().compareTo(y.region().path().toString());
    };

    /** 返回当前集群状态快照。 */
    ClusterEvent.CurrentClusterState clusterState();

    /** 返回本节点集群成员信息。 */
    Member selfMember();

    /**
     * 判断当前集群状态是否适合执行再平衡（所有活跃成员可达且无节点处于 Joining）。
     */
    default boolean isAGoodTimeToRebalance(List<RegionEntry> regionEntries) {
        if (regionEntries.isEmpty()) {
            return false;
        }
        ClusterEvent.CurrentClusterState state = clusterState();
        for (Member m : state.members()) {
            if (state.unreachable().contains(m)) {
                return false;
            }
            if (JOINING_CLUSTER.contains(m.status())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将当前分片分配映射转换为带有集群成员信息的 {@link RegionEntry} 列表。
     */
    default List<RegionEntry> regionEntriesFor(Map<ActorRef, List<String>> currentShardAllocations) {
        ClusterEvent.CurrentClusterState state = clusterState();
        Map<Address, Member> addressToMember = new HashMap<>();
        for (Member m : state.members()) {
            addressToMember.put(m.address(), m);
        }
        List<RegionEntry> entries = new ArrayList<>();
        for (Map.Entry<ActorRef, List<String>> entry : currentShardAllocations.entrySet()) {
            ActorRef region = entry.getKey();
            Address addr = region.path().address();
            Member member = addr.host() == null
                    ? selfMember()
                    : addressToMember.get(addr);
            if (member != null && member.status() != MemberStatus.Removed) {
                entries.add(new RegionEntry(region, member, entry.getValue()));
            }
        }
        return entries;
    }
}
