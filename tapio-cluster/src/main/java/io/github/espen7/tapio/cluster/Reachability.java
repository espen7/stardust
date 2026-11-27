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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * INTERNAL API：不可变的可达性状态表，记录观察者节点对被观察节点的可达性视图与单调版本号。
 */
@InternalApi
public final class Reachability implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 可达性状态枚举。 */
    public enum ReachabilityStatus {
        Reachable, Unreachable, Terminated
    }

    /**
     * 单条可达性记录。
     *
     * @param observer 观察者节点
     * @param subject 被观察节点
     * @param status 可达性状态
     * @param version 观察者递增版本号
     */
    public record Record(
            UniqueAddress observer,
            UniqueAddress subject,
            ReachabilityStatus status,
            long version) implements Serializable {
    }

    /** 空可达性表（全员可达）。 */
    public static final Reachability EMPTY = new Reachability(List.of(), Map.of());

    private final List<Record> records;
    private final Map<UniqueAddress, Long> versions;
    private transient volatile Set<UniqueAddress> cachedAllUnreachable;
    private transient volatile Set<UniqueAddress> cachedAllTerminated;

    /**
     * 构造可达性表。
     *
     * @param records 记录列表
     * @param versions 观察者版本号映射
     */
    public Reachability(List<Record> records, Map<UniqueAddress, Long> versions) {
        this.records = List.copyOf(Objects.requireNonNull(records, "records"));
        this.versions = Map.copyOf(Objects.requireNonNull(versions, "versions"));
    }

    /** 返回所有记录。 */
    public List<Record> records() {
        return records;
    }

    /** 返回观察者版本号映射。 */
    public Map<UniqueAddress, Long> versions() {
        return versions;
    }

    /** 将 {@code subject} 在 {@code observer} 视角下标记为不可达。 */
    public Reachability unreachable(UniqueAddress observer, UniqueAddress subject) {
        return change(observer, subject, ReachabilityStatus.Unreachable);
    }

    /** 将 {@code subject} 在 {@code observer} 视角下标记为可达。 */
    public Reachability reachable(UniqueAddress observer, UniqueAddress subject) {
        return change(observer, subject, ReachabilityStatus.Reachable);
    }

    /** 将 {@code subject} 在 {@code observer} 视角下标记为已终止。 */
    public Reachability terminated(UniqueAddress observer, UniqueAddress subject) {
        return change(observer, subject, ReachabilityStatus.Terminated);
    }

    private Reachability change(UniqueAddress observer, UniqueAddress subject, ReachabilityStatus status) {
        long nextVersion = versions.getOrDefault(observer, 0L) + 1L;
        Map<UniqueAddress, Long> nextVersions = new HashMap<>(versions);
        nextVersions.put(observer, nextVersion);

        List<Record> nextRecords = new ArrayList<>();
        boolean found = false;
        for (Record r : records) {
            if (r.observer().equals(observer) && r.subject().equals(subject)) {
                found = true;
                if (status != ReachabilityStatus.Reachable) {
                    nextRecords.add(new Record(observer, subject, status, nextVersion));
                }
            } else {
                nextRecords.add(r);
            }
        }
        if (!found && status != ReachabilityStatus.Reachable) {
            nextRecords.add(new Record(observer, subject, status, nextVersion));
        }
        return new Reachability(nextRecords, nextVersions);
    }

    /**
     * 按观察者版本号合并两份可达性表。
     *
     * @param allowed 允许保留的成员集合
     * @param other 另一份可达性表
     * @return 合并后的可达性表
     */
    public Reachability merge(Set<UniqueAddress> allowed, Reachability other) {
        Map<UniqueAddress, Long> mergedVersions = new HashMap<>();
        Set<UniqueAddress> allObservers = new HashSet<>(this.versions.keySet());
        allObservers.addAll(other.versions.keySet());
        allObservers.retainAll(allowed);

        List<Record> mergedRecords = new ArrayList<>();
        for (UniqueAddress obs : allObservers) {
            long v1 = this.versions.getOrDefault(obs, 0L);
            long v2 = other.versions.getOrDefault(obs, 0L);
            Reachability winner = v1 >= v2 ? this : other;
            long winVer = Math.max(v1, v2);
            if (winVer > 0) {
                mergedVersions.put(obs, winVer);
            }
            for (Record r : winner.records) {
                if (r.observer().equals(obs) && allowed.contains(r.subject())) {
                    mergedRecords.add(r);
                }
            }
        }
        return new Reachability(mergedRecords, mergedVersions);
    }

    /**
     * 移除已退出成员的所有观察与被观察记录。
     *
     * @param nodes 要移除的节点集合
     * @return 清理后的可达性表
     */
    public Reachability remove(Set<UniqueAddress> nodes) {
        if (records.isEmpty() && versions.isEmpty()) {
            return this;
        }
        List<Record> nextRecords = records.stream()
                .filter(r -> !nodes.contains(r.observer()) && !nodes.contains(r.subject()))
                .toList();
        Map<UniqueAddress, Long> nextVersions = new HashMap<>(versions);
        nodes.forEach(nextVersions::remove);
        return new Reachability(nextRecords, nextVersions);
    }

    /** 返回所有被任一观察者判定为不可达（且未终止）的节点集合。 */
    public Set<UniqueAddress> allUnreachable() {
        ensureCache();
        return cachedAllUnreachable;
    }

    /** 返回所有被任一观察者判定为已终止的节点集合。 */
    public Set<UniqueAddress> allTerminated() {
        ensureCache();
        return cachedAllTerminated;
    }

    /** 返回不可达或已终止的节点并集。 */
    public Set<UniqueAddress> allUnreachableOrTerminated() {
        ensureCache();
        if (cachedAllTerminated.isEmpty()) {
            return cachedAllUnreachable;
        }
        Set<UniqueAddress> union = new HashSet<>(cachedAllUnreachable);
        union.addAll(cachedAllTerminated);
        return Collections.unmodifiableSet(union);
    }

    /** 判断指定节点当前是否被所有观察者视为可达。 */
    public boolean isReachable(UniqueAddress node) {
        return !allUnreachableOrTerminated().contains(node);
    }

    /** 返回所有作为观察者出现的节点集合。 */
    public Set<UniqueAddress> allObservers() {
        Set<UniqueAddress> set = new HashSet<>();
        for (Record r : records) {
            set.add(r.observer());
        }
        return Collections.unmodifiableSet(set);
    }

    private void ensureCache() {
        if (cachedAllUnreachable != null) {
            return;
        }
        Set<UniqueAddress> term = new HashSet<>();
        Set<UniqueAddress> unreach = new HashSet<>();
        for (Record r : records) {
            if (r.status() == ReachabilityStatus.Terminated) {
                term.add(r.subject());
            } else if (r.status() == ReachabilityStatus.Unreachable) {
                unreach.add(r.subject());
            }
        }
        unreach.removeAll(term);
        cachedAllTerminated = Collections.unmodifiableSet(term);
        cachedAllUnreachable = Collections.unmodifiableSet(unreach);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Reachability other
                && records.equals(other.records)
                && versions.equals(other.versions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, versions);
    }
}
