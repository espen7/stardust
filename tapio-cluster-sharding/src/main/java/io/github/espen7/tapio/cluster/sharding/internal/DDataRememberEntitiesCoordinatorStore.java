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

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ddata.ORSet;
import io.github.espen7.tapio.cluster.ddata.ORSetKey;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.SelfUniqueAddress;
import io.github.espen7.tapio.cluster.sharding.ClusterShardingSettings;

import java.util.Optional;
import java.util.Set;

/**
 * INTERNAL API：基于 DData 的协调器记住分片存储 Actor。
 */
@InternalApi
public final class DDataRememberEntitiesCoordinatorStore extends AbstractLoggingActor {

    public static Props props(
            String typeName,
            ClusterShardingSettings settings,
            ActorRef replicator,
            int majorityMinCap) {
        return Props.create(DDataRememberEntitiesCoordinatorStore.class,
                () -> new DDataRememberEntitiesCoordinatorStore(typeName, settings, replicator, majorityMinCap));
    }

    private final ActorRef replicator;
    private final SelfUniqueAddress selfUniqueAddress;
    private final ORSetKey<String> allShardsKey;
    private final Replicator.ReadConsistency readConsistency;
    private final Replicator.WriteConsistency writeConsistency;

    private Set<String> allShards;
    private ActorRef coordinatorWaitingForShards;

    public DDataRememberEntitiesCoordinatorStore(
            String typeName,
            ClusterShardingSettings settings,
            ActorRef replicator,
            int majorityMinCap) {
        this.replicator = replicator;
        Cluster cluster = Cluster.get(getContext().getSystem());
        this.selfUniqueAddress = new SelfUniqueAddress(cluster.selfUniqueAddress());
        this.allShardsKey = ORSetKey.create("shard-" + typeName + "-all");
        this.readConsistency = settings.tuningParameters().coordinatorStateReadMajorityPlus() == Integer.MAX_VALUE
                ? new Replicator.ReadAll(settings.tuningParameters().waitingForStateTimeout())
                : new Replicator.ReadMajority(settings.tuningParameters().waitingForStateTimeout(), majorityMinCap);
        this.writeConsistency = settings.tuningParameters().coordinatorStateWriteMajorityPlus() == Integer.MAX_VALUE
                ? new Replicator.WriteAll(settings.tuningParameters().updatingStateTimeout())
                : new Replicator.WriteMajority(settings.tuningParameters().updatingStateTimeout(), majorityMinCap);
    }

    @Override
    public void preStart() {
        replicator.tell(new Replicator.Get<>(allShardsKey, readConsistency, Optional.empty()), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RememberEntitiesStore.CoordinatorStore.GetShards.class, msg -> {
                    coordinatorWaitingForShards = getSender();
                    if (allShards != null) {
                        coordinatorWaitingForShards.tell(
                                new RememberEntitiesStore.CoordinatorStore.RememberedShards(allShards), getSelf());
                        coordinatorWaitingForShards = null;
                    }
                })
                .match(Replicator.GetSuccess.class, gs -> {
                    @SuppressWarnings("unchecked")
                    ORSet<String> set = (ORSet<String>) gs.dataValue();
                    onGotAllShards(set.elements());
                })
                .match(Replicator.NotFound.class, nf -> onGotAllShards(Set.of()))
                .match(Replicator.GetFailure.class, gf ->
                        replicator.tell(new Replicator.Get<>(allShardsKey, readConsistency, Optional.empty()), getSelf()))
                .match(RememberEntitiesStore.CoordinatorStore.AddShard.class, add -> {
                    ActorRef replyTo = getSender();
                    replicator.tell(new Replicator.Update<>(
                            allShardsKey,
                            ORSet.create(),
                            writeConsistency,
                            Optional.of(new AddRequest(add.shardId(), replyTo)),
                            set -> set.add(selfUniqueAddress, add.shardId())), getSelf());
                })
                .match(Replicator.UpdateSuccess.class, us -> {
                    if (us.request().isPresent() && us.request().get() instanceof AddRequest req) {
                        req.replyTo().tell(
                                new RememberEntitiesStore.CoordinatorStore.UpdateDone(req.shardId()), getSelf());
                    }
                })
                .match(Replicator.UpdateTimeout.class, ut -> {
                    if (ut.request().isPresent() && ut.request().get() instanceof AddRequest req) {
                        req.replyTo().tell(
                                new RememberEntitiesStore.CoordinatorStore.UpdateFailed(req.shardId()), getSelf());
                    }
                })
                .build();
    }

    private void onGotAllShards(Set<String> shards) {
        this.allShards = shards;
        if (coordinatorWaitingForShards != null) {
            coordinatorWaitingForShards.tell(
                    new RememberEntitiesStore.CoordinatorStore.RememberedShards(shards), getSelf());
            coordinatorWaitingForShards = null;
        }
    }

    private record AddRequest(String shardId, ActorRef replyTo) {
    }
}
