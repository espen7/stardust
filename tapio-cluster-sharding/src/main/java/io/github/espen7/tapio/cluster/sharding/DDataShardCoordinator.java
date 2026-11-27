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
package io.github.espen7.tapio.cluster.sharding;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ddata.LWWRegister;
import io.github.espen7.tapio.cluster.ddata.LWWRegisterKey;
import io.github.espen7.tapio.cluster.ddata.Replicator;
import io.github.espen7.tapio.cluster.ddata.SelfUniqueAddress;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;

import java.util.Optional;

/**
 * INTERNAL API：基于 DData {@link LWWRegister} 存储状态的集群分片协调器实现。
 */
@InternalApi
public final class DDataShardCoordinator extends ShardCoordinator {

    private final ActorRef replicator;
    private final SelfUniqueAddress selfUniqueAddress;
    private final LWWRegisterKey<State> stateKey;
    private final Replicator.ReadConsistency readConsistency;
    private final Replicator.WriteConsistency writeConsistency;
    private final ActorRef rememberEntitiesStore;

    public DDataShardCoordinator(
            String typeName,
            ClusterShardingSettings settings,
            ShardAllocationStrategy allocationStrategy,
            ActorRef replicator,
            int majorityMinCap,
            RememberEntitiesStore.Provider rememberEntitiesProvider) {
        super(typeName, settings, allocationStrategy);
        this.replicator = replicator;
        Cluster cluster = Cluster.get(getContext().getSystem());
        this.selfUniqueAddress = new SelfUniqueAddress(cluster.selfUniqueAddress());
        this.stateKey = LWWRegisterKey.create("shard-coordinator-" + typeName);
        this.readConsistency = settings.tuningParameters().coordinatorStateReadMajorityPlus() == Integer.MAX_VALUE
                ? new Replicator.ReadAll(settings.tuningParameters().waitingForStateTimeout())
                : new Replicator.ReadMajority(settings.tuningParameters().waitingForStateTimeout(), majorityMinCap);
        this.writeConsistency = settings.tuningParameters().coordinatorStateWriteMajorityPlus() == Integer.MAX_VALUE
                ? new Replicator.WriteAll(settings.tuningParameters().updatingStateTimeout())
                : new Replicator.WriteMajority(settings.tuningParameters().updatingStateTimeout(), majorityMinCap);
        this.rememberEntitiesStore = rememberEntitiesProvider == null
                ? null
                : getContext().actorOf(rememberEntitiesProvider.coordinatorStoreProps(), "rememberEntitiesStore");
    }

    @Override
    public void preStart() {
        super.preStart();
        replicator.tell(new Replicator.Get<>(stateKey, readConsistency, Optional.empty()), getSelf());
        if (rememberEntitiesStore != null) {
            rememberEntitiesStore.tell(RememberEntitiesStore.CoordinatorStore.GetShards.INSTANCE, getSelf());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Replicator.GetSuccess.class, gs -> {
                    @SuppressWarnings("unchecked")
                    LWWRegister<State> reg = (LWWRegister<State>) gs.dataValue();
                    onStateInitialized(reg.value());
                })
                .match(Replicator.NotFound.class, nf -> onStateInitialized(State.EMPTY))
                .match(Replicator.GetFailure.class, gf ->
                        replicator.tell(new Replicator.Get<>(stateKey, readConsistency, Optional.empty()), getSelf()))
                .match(RememberEntitiesStore.CoordinatorStore.RememberedShards.class, rs -> {
                    unallocatedToRestart.addAll(rs.shards());
                    if (stateInitialized) {
                        restartRememberedShards();
                    }
                })
                .match(Internal.Terminate.class, msg -> getContext().stop(getSelf()))
                .match(Replicator.UpdateSuccess.class, ignored -> { })
                .match(Replicator.UpdateTimeout.class, ignored -> { })
                .match(RememberEntitiesStore.CoordinatorStore.UpdateDone.class, ignored -> { })
                .match(RememberEntitiesStore.CoordinatorStore.UpdateFailed.class, ignored -> { })
                .matchAny(msg -> {
                    if (stateInitialized) {
                        if (!handleActive(msg)) {
                            unhandled(msg);
                        }
                    } else {
                        stashOrHandleActive(msg);
                    }
                })
                .build();
    }

    @Override
    protected void onShardAllocated(String shardId) {
        if (rememberEntitiesStore != null) {
            rememberEntitiesStore.tell(new RememberEntitiesStore.CoordinatorStore.AddShard(shardId), getSelf());
        }
    }

    @Override
    protected void persistState() {
        replicator.tell(new Replicator.Update<>(
                stateKey,
                LWWRegister.create(selfUniqueAddress, state),
                writeConsistency,
                Optional.empty(),
                reg -> reg.withValue(selfUniqueAddress, state)), getSelf());
    }
}
