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
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * INTERNAL API：维护集群当前只读快照状态，供 {@link Cluster#state()} 快速读取。
 */
@InternalApi
public final class ClusterReadView {
    private volatile Gossip latestGossip = Gossip.EMPTY;
    private volatile ClusterEvent.CurrentClusterState state = ClusterEvent.CurrentClusterState.EMPTY;
    private volatile Member selfMember;

    /**
     * 构造只读视图。
     *
     * @param selfUniqueAddress 本节点唯一地址
     * @param roles 本节点角色集合
     * @param appVersion 应用版本
     */
    public ClusterReadView(UniqueAddress selfUniqueAddress, Set<String> roles, String appVersion) {
        this.selfMember = Member.create(selfUniqueAddress, roles, appVersion);
    }

    /** 更新最新 Gossip 与派生 {@link ClusterEvent.CurrentClusterState}。 */
    public void refresh(Gossip gossip, UniqueAddress selfUniqueAddress, String selfDataCenter) {
        this.latestGossip = gossip;
        if (gossip.hasMember(selfUniqueAddress)) {
            this.selfMember = gossip.member(selfUniqueAddress);
        }
        Set<UniqueAddress> unreachUas = gossip.overview().reachability().allUnreachableOrTerminated();
        Set<Member> unreachMembers = new HashSet<>();
        for (UniqueAddress ua : unreachUas) {
            if (gossip.hasMember(ua)) {
                unreachMembers.add(gossip.member(ua));
            }
        }
        Set<Address> seenBy = new HashSet<>();
        for (UniqueAddress ua : gossip.overview().seen()) {
            seenBy.add(ua.address());
        }
        Optional<Address> leader = gossip.leaderOf(selfDataCenter).map(UniqueAddress::address);
        Map<String, Optional<Address>> roleLeaders = new HashMap<>();
        for (String role : gossip.allRoles()) {
            roleLeaders.put(role, gossip.roleLeaderOf(role, selfDataCenter).map(UniqueAddress::address));
        }
        this.state = new ClusterEvent.CurrentClusterState(
                gossip.members(), unreachMembers, seenBy, leader, roleLeaders);
    }

    /** 返回当前集群状态快照。 */
    public ClusterEvent.CurrentClusterState state() {
        return state;
    }

    /** 返回最新 Gossip。 */
    public Gossip latestGossip() {
        return latestGossip;
    }

    /** 返回本节点当前 {@link Member} 视图。 */
    public Member selfMember() {
        return selfMember;
    }
}
