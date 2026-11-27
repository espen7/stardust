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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * INTERNAL API：Gossip 概览信息，记录已确认当前状态的 {@code seen} 集合与可达性表。
 *
 * @param seen 已见过当前 Gossip 版本的成员唯一地址集合
 * @param reachability 成员间可达性状态表
 */
@InternalApi
public record GossipOverview(Set<UniqueAddress> seen, Reachability reachability) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 空概览实例。 */
    public static final GossipOverview EMPTY = new GossipOverview(Set.of(), Reachability.EMPTY);

    /**
     * 防御性复制集合。
     *
     * @param seen 已见集合
     * @param reachability 可达性表
     */
    public GossipOverview {
        seen = Set.copyOf(Objects.requireNonNull(seen, "seen"));
        reachability = Objects.requireNonNull(reachability, "reachability");
    }

    /** 返回添加了 {@code node} 后的新概览。 */
    public GossipOverview seen(UniqueAddress node) {
        if (seen.contains(node)) {
            return this;
        }
        Set<UniqueAddress> next = new HashSet<>(seen);
        next.add(node);
        return new GossipOverview(next, reachability);
    }

    /** 返回仅保留 {@code members} 中存在节点的新概览。 */
    public GossipOverview prune(Set<UniqueAddress> members) {
        Set<UniqueAddress> nextSeen = new HashSet<>(seen);
        nextSeen.retainAll(members);
        return new GossipOverview(Collections.unmodifiableSet(nextSeen), reachability);
    }
}
