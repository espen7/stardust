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
package io.github.espen7.tapio.cluster.sharding.external;

import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.cluster.ddata.DistributedData;
import io.github.espen7.tapio.cluster.ddata.LWWRegister;
import io.github.espen7.tapio.cluster.ddata.LWWRegisterKey;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.SelfUniqueAddress;
import io.github.espen7.tapio.pattern.Patterns;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 外部分片分配扩展，提供按实体类型获取 {@link ExternalShardAllocationClient} 的入口。
 */
public final class ExternalShardAllocation implements Extension {

    /** 扩展 ID。 */
    public static final AbstractExtensionId<ExternalShardAllocation> Id = new AbstractExtensionId<>() {
        @Override
        public ExternalShardAllocation createExtension(ExtendedActorSystem system) {
            return new ExternalShardAllocation(system);
        }
    };

    public static ExternalShardAllocation get(ActorSystem system) {
        return Id.apply(system);
    }

    private final ExtendedActorSystem system;
    private final ConcurrentHashMap<String, ExternalShardAllocationClient> clients = new ConcurrentHashMap<>();

    private ExternalShardAllocation(ExtendedActorSystem system) {
        this.system = system;
    }

    /**
     * 获取指定实体类型的 {@link ExternalShardAllocationClient}。
     */
    public ExternalShardAllocationClient clientFor(String typeName) {
        return clients.computeIfAbsent(typeName, this::createClient);
    }

    static LWWRegisterKey<ShardLocations> keyFor(String typeName) {
        return LWWRegisterKey.create("shard-locations-" + typeName);
    }

    private ExternalShardAllocationClient createClient(String typeName) {
        DistributedData ddata = DistributedData.get(system);
        ActorRef replicator = ddata.replicator();
        SelfUniqueAddress selfUa = ddata.selfUniqueAddress();
        LWWRegisterKey<ShardLocations> key = keyFor(typeName);
        Duration timeout = system.settings().config()
                .getDuration("tapio.cluster.sharding.updating-state-timeout");

        return new ExternalShardAllocationClient() {
            @Override
            public CompletionStage<Done> updateShardLocation(String shardId, Address location) {
                return updateShardLocations(Map.of(shardId, location));
            }

            @Override
            public CompletionStage<Done> updateShardLocations(Map<String, Address> locations) {
                var update = new Replicator.Update<>(
                        key,
                        LWWRegister.create(selfUa, ShardLocations.EMPTY),
                        new Replicator.WriteMajority(timeout),
                        Optional.empty(),
                        reg -> reg.withValue(selfUa, reg.value().withUpdated(locations)));
                return Patterns.ask(replicator, update, timeout).thenCompose(resp -> {
                    if (resp instanceof Replicator.UpdateSuccess<?>) {
                        return CompletableFuture.completedFuture(Done.getInstance());
                    }
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Failed to update shard locations: " + resp));
                });
            }
        };
    }
}
