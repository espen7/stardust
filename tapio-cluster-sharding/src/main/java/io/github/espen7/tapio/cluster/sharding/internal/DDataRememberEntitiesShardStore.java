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
 * INTERNAL API：基于 DData 的分片记住实体存储 Actor。
 */
@InternalApi
public final class DDataRememberEntitiesShardStore extends AbstractLoggingActor {

    public static Props props(
            String shardId,
            String typeName,
            ClusterShardingSettings settings,
            ActorRef replicator,
            int majorityMinCap) {
        return Props.create(DDataRememberEntitiesShardStore.class,
                () -> new DDataRememberEntitiesShardStore(shardId, typeName, settings, replicator, majorityMinCap));
    }

    private final ActorRef replicator;
    private final SelfUniqueAddress selfUniqueAddress;
    private final ORSetKey<String> entitiesKey;
    private final Replicator.ReadConsistency readConsistency;
    private final Replicator.WriteConsistency writeConsistency;

    private Set<String> rememberedEntities;
    private ActorRef waitingForEntities;

    public DDataRememberEntitiesShardStore(
            String shardId,
            String typeName,
            ClusterShardingSettings settings,
            ActorRef replicator,
            int majorityMinCap) {
        this.replicator = replicator;
        Cluster cluster = Cluster.get(getContext().getSystem());
        this.selfUniqueAddress = new SelfUniqueAddress(cluster.selfUniqueAddress());
        this.entitiesKey = ORSetKey.create("shard-" + typeName + "-" + shardId);
        this.readConsistency = new Replicator.ReadMajority(
                settings.tuningParameters().waitingForStateTimeout(), majorityMinCap);
        this.writeConsistency = new Replicator.WriteMajority(
                settings.tuningParameters().updatingStateTimeout(), majorityMinCap);
    }

    @Override
    public void preStart() {
        replicator.tell(new Replicator.Get<>(entitiesKey, readConsistency, Optional.empty()), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RememberEntitiesStore.ShardStore.GetEntities.class, msg -> {
                    waitingForEntities = getSender();
                    if (rememberedEntities != null) {
                        waitingForEntities.tell(
                                new RememberEntitiesStore.ShardStore.RememberedEntities(rememberedEntities), getSelf());
                        waitingForEntities = null;
                    }
                })
                .match(Replicator.GetSuccess.class, gs -> {
                    @SuppressWarnings("unchecked")
                    ORSet<String> set = (ORSet<String>) gs.dataValue();
                    onGotEntities(set.elements());
                })
                .match(Replicator.NotFound.class, nf -> onGotEntities(Set.of()))
                .match(Replicator.GetFailure.class, gf ->
                        replicator.tell(new Replicator.Get<>(entitiesKey, readConsistency, Optional.empty()), getSelf()))
                .match(RememberEntitiesStore.ShardStore.Update.class, upd -> {
                    ActorRef replyTo = getSender();
                    replicator.tell(new Replicator.Update<>(
                            entitiesKey,
                            ORSet.create(),
                            writeConsistency,
                            Optional.of(new UpdateRequest(upd.started(), upd.stopped(), replyTo)),
                            set -> {
                                ORSet<String> next = set;
                                for (String id : upd.started()) {
                                    next = next.add(selfUniqueAddress, id);
                                }
                                for (String id : upd.stopped()) {
                                    next = next.remove(selfUniqueAddress, id);
                                }
                                return next;
                            }), getSelf());
                })
                .match(Replicator.UpdateSuccess.class, us -> {
                    if (us.request().isPresent() && us.request().get() instanceof UpdateRequest req) {
                        req.replyTo().tell(
                                new RememberEntitiesStore.ShardStore.UpdateDone(req.started(), req.stopped()),
                                getSelf());
                    }
                })
                .match(Replicator.UpdateTimeout.class, ut -> {
                    if (ut.request().isPresent() && ut.request().get() instanceof UpdateRequest req) {
                        req.replyTo().tell(
                                new RememberEntitiesStore.ShardStore.UpdateDone(req.started(), req.stopped()),
                                getSelf());
                    }
                })
                .build();
    }

    private void onGotEntities(Set<String> entities) {
        this.rememberedEntities = entities;
        if (waitingForEntities != null) {
            waitingForEntities.tell(new RememberEntitiesStore.ShardStore.RememberedEntities(entities), getSelf());
            waitingForEntities = null;
        }
    }

    private record UpdateRequest(Set<String> started, Set<String> stopped, ActorRef replyTo) {
    }
}
