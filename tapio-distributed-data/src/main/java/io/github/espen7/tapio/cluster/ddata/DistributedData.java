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

import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.Optional;
import java.util.Set;

/**
 * 分布式数据（Distributed Data）扩展入口，提供共享 {@link Replicator} 引用与 {@link SelfUniqueAddress}。
 */
public final class DistributedData implements Extension {

    /** 扩展 ID 单例。 */
    public static final DistributedDataExtension INSTANCE = new DistributedDataExtension();

    /** 扩展提供者实现。 */
    public static final class DistributedDataExtension extends AbstractExtensionId<DistributedData>
            implements ExtensionIdProvider {
        private DistributedDataExtension() {
        }

        @Override
        public ExtensionId<DistributedData> lookup() {
            return INSTANCE;
        }

        @Override
        public DistributedData createExtension(ExtendedActorSystem system) {
            return new DistributedData(system);
        }
    }

    /** 获取给定系统的 {@link DistributedData} 扩展。 */
    public static DistributedData get(ActorSystem system) {
        return INSTANCE.apply(system);
    }

    private final ReplicatorSettings settings;
    private final SelfUniqueAddress selfUniqueAddress;
    private final ActorRef replicator;
    private final boolean terminated;

    private DistributedData(ExtendedActorSystem system) {
        this.settings = ReplicatorSettings.create(system);
        Cluster cluster = Cluster.get(system);
        this.selfUniqueAddress = new SelfUniqueAddress(cluster.selfUniqueAddress());
        Set<String> requiredRoles = settings.role().isEmpty() ? Set.of() : Set.of(settings.role());
        this.terminated = cluster.isTerminated() || !cluster.selfRoles().containsAll(requiredRoles);
        if (this.terminated) {
            LoggingAdapter log = Logging.getLogger(system, this, getClass());
            log.warning("Replicator points to dead letters. Node roles [{}], required [{}].",
                    cluster.selfRoles(), requiredRoles);
            this.replicator = system.deadLetters();
        } else {
            this.replicator = system.systemActorOf(
                    Replicator.props(settings), ReplicatorSettings.name(system, Optional.empty()));
        }
    }

    /** 返回本节点 {@link Replicator} 引用。 */
    public ActorRef replicator() {
        return replicator;
    }

    /** 返回本节点 {@link SelfUniqueAddress}。 */
    public SelfUniqueAddress selfUniqueAddress() {
        return selfUniqueAddress;
    }

    /** 返回当前节点是否未启用或已终止分布式数据副本。 */
    public boolean isTerminated() {
        return terminated;
    }
}
