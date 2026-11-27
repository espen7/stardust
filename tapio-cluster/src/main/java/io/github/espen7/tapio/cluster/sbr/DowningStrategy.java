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
package io.github.espen7.tapio.cluster.sbr;

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.cluster.Reachability;
import io.github.espen7.tapio.cluster.UniqueAddress;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * INTERNAL API：脑裂裁决策略抽象基类（含间接触达节点拓扑检测）。
 */
@InternalApi
public abstract class DowningStrategy {

    /** 脑裂裁决结论。 */
    public enum Decision {
        DOWN_REACHABLE,
        DOWN_UNREACHABLE,
        DOWN_ALL,
        DOWN_INDIRECTLY_CONNECTED,
        REVERSE_DOWN_INDIRECTLY_CONNECTED
    }

    private final Optional<String> role;
    private Reachability reachability = Reachability.EMPTY;
    private Set<Address> seenBy = Set.of();

    protected DowningStrategy(Optional<String> role) {
        this.role = role;
    }

    /** 返回该策略限定的节点角色。 */
    public final Optional<String> role() {
        return role;
    }

    /** 更新当前集群可达性矩阵（仅保留 Unreachable / Terminated 记录）。 */
    public final void setReachability(Reachability r) {
        if (r == null) {
            this.reachability = Reachability.EMPTY;
            return;
        }
        List<Reachability.Record> filtered = r.records().stream()
                .filter(rec -> rec.status() == Reachability.ReachabilityStatus.Unreachable
                        || rec.status() == Reachability.ReachabilityStatus.Terminated)
                .toList();
        this.reachability = new Reachability(filtered, r.versions());
    }

    /** 更新当前 Gossip 已确认集合。 */
    public final void setSeenBy(Set<Address> seenBy) {
        this.seenBy = seenBy == null ? Set.of() : Set.copyOf(seenBy);
    }

    /**
     * 计算被标记为不可达但仍可通过第三方节点间接通信的节点集合（间接触达环或已看到最新 Gossip）。
     */
    public final Set<UniqueAddress> indirectlyConnected() {
        Set<UniqueAddress> result = new HashSet<>(indirectlyConnectedFromIntersectionOfObserversAndSubjects());
        result.addAll(indirectlyConnectedFromSeenCurrentGossip());
        return Set.copyOf(result);
    }

    private Set<UniqueAddress> indirectlyConnectedFromIntersectionOfObserversAndSubjects() {
        Set<UniqueAddress> observers = new HashSet<>();
        Set<UniqueAddress> subjects = new HashSet<>();
        for (Reachability.Record r : reachability.records()) {
            observers.add(r.observer());
            subjects.add(r.subject());
        }
        observers.retainAll(subjects);
        return observers;
    }

    private Set<UniqueAddress> indirectlyConnectedFromSeenCurrentGossip() {
        Set<UniqueAddress> result = new HashSet<>();
        for (Reachability.Record r : reachability.records()) {
            if (seenBy.contains(r.subject().address())) {
                result.add(r.observer());
                result.add(r.subject());
            }
        }
        return result;
    }

    /** 是否存在间接触达节点。 */
    public final boolean hasIndirectlyConnected() {
        return !indirectlyConnected().isEmpty();
    }

    /**
     * 基于当前成员集合与不可达成员集合计算需要执行 Down 的目标节点列表。
     */
    public final List<Member> decideAndSelect(Set<Member> members, Set<Member> unreachable) {
        List<Member> eligible = new ArrayList<>();
        List<Member> reachableEligible = new ArrayList<>();
        List<Member> unreachableEligible = new ArrayList<>();
        for (Member m : members) {
            if (m.status() == MemberStatus.Down || m.status() == MemberStatus.Exiting) {
                continue;
            }
            if (role.isEmpty() || m.hasRole(role.get())) {
                eligible.add(m);
                if (unreachable.contains(m)) {
                    unreachableEligible.add(m);
                } else {
                    reachableEligible.add(m);
                }
            }
        }
        Decision decision = decide(eligible, reachableEligible, unreachableEligible);
        Set<UniqueAddress> indirect = indirectlyConnected();
        if (!indirect.isEmpty()) {
            if (decision == Decision.DOWN_UNREACHABLE) {
                decision = Decision.DOWN_INDIRECTLY_CONNECTED;
            } else if (decision == Decision.DOWN_REACHABLE) {
                decision = Decision.REVERSE_DOWN_INDIRECTLY_CONNECTED;
            }
        }
        return switch (decision) {
            case DOWN_UNREACHABLE -> List.copyOf(unreachable);
            case DOWN_REACHABLE -> members.stream().filter(m -> !unreachable.contains(m)).toList();
            case DOWN_ALL -> new ArrayList<>(members);
            case DOWN_INDIRECTLY_CONNECTED -> members.stream()
                    .filter(m -> indirect.contains(m.uniqueAddress()) || unreachable.contains(m))
                    .toList();
            case REVERSE_DOWN_INDIRECTLY_CONNECTED -> members.stream()
                    .filter(m -> indirect.contains(m.uniqueAddress()) || !unreachable.contains(m))
                    .toList();
        };
    }

    /**
     * 由具体策略子类实现的裁决逻辑。
     */
    protected abstract Decision decide(
            List<Member> eligible,
            List<Member> reachableEligible,
            List<Member> unreachableEligible);
}
