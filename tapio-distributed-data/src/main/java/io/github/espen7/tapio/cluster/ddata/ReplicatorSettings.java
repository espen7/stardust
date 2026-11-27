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
package io.github.espen7.tapio.cluster.ddata;

import com.typesafe.config.Config;
import io.github.espen7.tapio.actor.ActorSystem;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * {@link Replicator} 配置参数。
 */
public final class ReplicatorSettings {
    private final Set<String> roles;
    private final Duration gossipInterval;
    private final Duration notifySubscribersInterval;
    private final String dispatcher;

    private ReplicatorSettings(
            Set<String> roles,
            Duration gossipInterval,
            Duration notifySubscribersInterval,
            String dispatcher) {
        this.roles = Collections.unmodifiableSet(new HashSet<>(roles));
        this.gossipInterval = gossipInterval;
        this.notifySubscribersInterval = notifySubscribersInterval;
        this.dispatcher = dispatcher;
    }

    /** 从系统配置 {@code tapio.cluster.distributed-data} 创建。 */
    public static ReplicatorSettings create(ActorSystem system) {
        return create(system.settings().config().getConfig("tapio.cluster.distributed-data"));
    }

    /** 从指定配置节创建。 */
    public static ReplicatorSettings create(Config config) {
        String r = config.hasPath("role") ? config.getString("role") : "";
        Set<String> roles = (r == null || r.isEmpty()) ? Set.of() : Set.of(r);
        return new ReplicatorSettings(
                roles,
                config.getDuration("gossip-interval"),
                config.getDuration("notify-subscribers-interval"),
                config.getString("use-dispatcher"));
    }

    /** 返回命名策略对应的 Replicator Actor 名称。 */
    public static String name(ActorSystem system, Optional<String> role) {
        String base = system.settings().config().getString("tapio.cluster.distributed-data.name");
        return role.filter(r -> !r.isEmpty()).map(r -> base + "-" + r).orElse(base);
    }

    /** 返回单个角色（若配置了多个角色则返回其中一个，未配置则返回空字符串）。 */
    public String role() {
        return roles.isEmpty() ? "" : roles.iterator().next();
    }

    /** 返回 Replicator 限定的全部节点角色集合。 */
    public Set<String> roles() {
        return roles;
    }

    public Duration gossipInterval() {
        return gossipInterval;
    }

    public Duration notifySubscribersInterval() {
        return notifySubscribersInterval;
    }

    public String dispatcher() {
        return dispatcher;
    }

    public ReplicatorSettings withRole(String newRole) {
        Set<String> next = (newRole == null || newRole.isEmpty()) ? Set.of() : Set.of(newRole);
        return new ReplicatorSettings(next, gossipInterval, notifySubscribersInterval, dispatcher);
    }

    public ReplicatorSettings withRoles(Set<String> newRoles) {
        Set<String> filtered = new HashSet<>();
        if (newRoles != null) {
            for (String r : newRoles) {
                if (r != null && !r.isEmpty()) {
                    filtered.add(r);
                }
            }
        }
        return new ReplicatorSettings(filtered, gossipInterval, notifySubscribersInterval, dispatcher);
    }

    public ReplicatorSettings withGossipInterval(Duration newInterval) {
        return new ReplicatorSettings(roles, newInterval, notifySubscribersInterval, dispatcher);
    }
}
