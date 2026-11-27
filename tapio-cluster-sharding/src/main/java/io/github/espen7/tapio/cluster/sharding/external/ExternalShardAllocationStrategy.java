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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ddata.DistributedData;
import io.github.espen7.tapio.cluster.ddata.LWWRegister;
import io.github.espen7.tapio.cluster.ddata.LWWRegisterKey;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.sharding.ShardAllocationStrategy;
import io.github.espen7.tapio.pattern.Patterns;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 基于 {@link ExternalShardAllocationClient} 显式指定目标节点的外部分片分配策略。
 */
public final class ExternalShardAllocationStrategy implements ShardAllocationStrategy {

    private record GetLocation(String shardId) {
    }

    private enum GetAllLocations {
        INSTANCE
    }

    private static final class DDataStateActor extends AbstractActor {
        private final LWWRegisterKey<ShardLocations> key;
        private ShardLocations latest = ShardLocations.EMPTY;

        DDataStateActor(String typeName) {
            this.key = ExternalShardAllocation.keyFor(typeName);
        }

        @Override
        public void preStart() {
            DistributedData.get(getContext().getSystem()).replicator()
                    .tell(new Replicator.Subscribe<>(key, getSelf()), getSelf());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Replicator.Changed.class, c -> {
                        if (c.key().equals(key)) {
                            latest = ((LWWRegister<ShardLocations>) c.dataValue()).value();
                        }
                    })
                    .match(GetLocation.class, g -> getSender().tell(
                            java.util.Optional.ofNullable(latest.locations().get(g.shardId())), getSelf()))
                    .match(GetAllLocations.class, g -> getSender().tell(latest, getSelf()))
                    .build();
        }
    }

    private final ActorSystem system;
    private final String typeName;
    private final Duration timeout;
    private final ActorRef stateActor;

    private ExternalShardAllocationStrategy(ActorSystem system, String typeName, Duration timeout) {
        this.system = system;
        this.typeName = typeName;
        this.timeout = timeout;
        this.stateActor = system.actorOf(
                Props.create(DDataStateActor.class, () -> new DDataStateActor(typeName)),
                "externalShardAllocation-" + typeName + "-" + java.util.UUID.randomUUID());
    }

    public static ExternalShardAllocationStrategy create(ActorSystem system, String typeName) {
        return new ExternalShardAllocationStrategy(system, typeName, Duration.ofSeconds(5));
    }

    public static ExternalShardAllocationStrategy create(ActorSystem system, String typeName, Duration timeout) {
        return new ExternalShardAllocationStrategy(system, typeName, timeout);
    }

    @Override
    public CompletionStage<ActorRef> allocateShard(
            ActorRef requester,
            String shardId,
            Map<ActorRef, List<String>> currentShardAllocations) {
        return Patterns.ask(stateActor, GetAllLocations.INSTANCE, timeout).thenApply(resp -> {
            ShardLocations locs = (ShardLocations) resp;
            Address desired = locs.locations().get(shardId);
            if (desired != null) {
                for (ActorRef region : currentShardAllocations.keySet()) {
                    if (regionAddress(region).equals(desired)) {
                        return region;
                    }
                }
            }
            return leastLoadedOrRequester(requester, currentShardAllocations);
        });
    }

    @Override
    public CompletionStage<Set<String>> rebalance(
            Map<ActorRef, List<String>> currentShardAllocations,
            Set<String> rebalanceInProgress) {
        if (!rebalanceInProgress.isEmpty() || currentShardAllocations.size() <= 1) {
            return CompletableFuture.completedFuture(Set.of());
        }
        return Patterns.ask(stateActor, GetAllLocations.INSTANCE, timeout).thenApply(resp -> {
            ShardLocations locs = (ShardLocations) resp;
            Set<Address> availableNodes = new HashSet<>();
            for (ActorRef r : currentShardAllocations.keySet()) {
                availableNodes.add(regionAddress(r));
            }
            Set<String> toMove = new HashSet<>();
            for (Map.Entry<ActorRef, List<String>> entry : currentShardAllocations.entrySet()) {
                Address currentAddr = regionAddress(entry.getKey());
                for (String shardId : entry.getValue()) {
                    Address desired = locs.locations().get(shardId);
                    if (desired != null && !desired.equals(currentAddr) && availableNodes.contains(desired)) {
                        toMove.add(shardId);
                    }
                }
            }
            return toMove;
        });
    }

    private Address regionAddress(ActorRef region) {
        Address addr = region.path().address();
        return addr.host() == null ? Cluster.get(system).selfAddress() : addr;
    }

    private static ActorRef leastLoadedOrRequester(
            ActorRef requester,
            Map<ActorRef, List<String>> currentShardAllocations) {
        ActorRef best = requester;
        int min = Integer.MAX_VALUE;
        for (Map.Entry<ActorRef, List<String>> entry : currentShardAllocations.entrySet()) {
            if (entry.getValue().size() < min) {
                min = entry.getValue().size();
                best = entry.getKey();
            }
        }
        return best;
    }
}
