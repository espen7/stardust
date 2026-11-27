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

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.annotation.InternalStableApi;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * 集群分片协调器抽象基类，负责维护分片与 Region 的分配映射并驱动再平衡。
 */
public abstract class ShardCoordinator extends AbstractLoggingActor {

    /**
     * INTERNAL API：协调器与 {@link ShardRegion} 之间的内部协议消息。
     */
    @InternalApi
    public static final class Internal {
        private Internal() {
        }

        /** 由 {@link ClusterSharding} 发送的协调器专用优雅停止消息。 */
        public enum Terminate implements DeadLetterSuppression {
            INSTANCE
        }

        public interface CoordinatorCommand extends ClusterShardingSerializable {
        }

        public interface CoordinatorMessage extends ClusterShardingSerializable {
        }

        public record Register(ActorRef shardRegion) implements CoordinatorCommand, DeadLetterSuppression {
        }

        public record RegisterProxy(ActorRef shardRegionProxy) implements CoordinatorCommand, DeadLetterSuppression {
        }

        public record RegisterAck(ActorRef coordinator) implements CoordinatorMessage {
        }

        public record GetShardHome(String shard) implements CoordinatorCommand, DeadLetterSuppression {
        }

        public record ShardHome(String shard, ActorRef ref) implements CoordinatorMessage {
        }

        public record ShardHomes(Map<ActorRef, List<String>> homes)
                implements CoordinatorMessage, DeadLetterSuppression {
        }

        public record HostShard(String shard) implements CoordinatorMessage {
        }

        public record ShardStarted(String shard) implements CoordinatorMessage {
        }

        public record BeginHandOff(String shard) implements CoordinatorMessage {
        }

        public record BeginHandOffAck(String shard) implements CoordinatorCommand {
        }

        public record HandOff(String shard) implements CoordinatorMessage {
        }

        public record ShardStopped(String shard) implements CoordinatorCommand {
        }

        public record ShardRegionTerminated(ActorRef region) implements CoordinatorCommand {
        }
    }

    /**
     * 协调器不可变状态快照。
     */
    public record State(
            Map<String, ActorRef> shards,
            Map<ActorRef, List<String>> regions,
            Set<ActorRef> regionProxies,
            Set<String> unallocatedShards) implements Serializable {

        public static final State EMPTY = new State(Map.of(), Map.of(), Set.of(), Set.of());

        public State withRegionRegistered(ActorRef region) {
            if (regions.containsKey(region)) {
                return this;
            }
            Map<ActorRef, List<String>> nextRegions = new HashMap<>(regions);
            nextRegions.put(region, List.of());
            return new State(shards, Collections.unmodifiableMap(nextRegions), regionProxies, unallocatedShards);
        }

        public State withProxyRegistered(ActorRef proxy) {
            if (regionProxies.contains(proxy)) {
                return this;
            }
            Set<ActorRef> nextProxies = new HashSet<>(regionProxies);
            nextProxies.add(proxy);
            return new State(shards, regions, Collections.unmodifiableSet(nextProxies), unallocatedShards);
        }

        public State withShardHomeAllocated(String shardId, ActorRef region) {
            Map<String, ActorRef> nextShards = new HashMap<>(shards);
            nextShards.put(shardId, region);

            Map<ActorRef, List<String>> nextRegions = new HashMap<>(regions);
            List<String> list = new ArrayList<>(nextRegions.getOrDefault(region, List.of()));
            if (!list.contains(shardId)) {
                list.add(shardId);
            }
            nextRegions.put(region, Collections.unmodifiableList(list));

            Set<String> nextUnallocated = new HashSet<>(unallocatedShards);
            nextUnallocated.remove(shardId);

            return new State(
                    Collections.unmodifiableMap(nextShards),
                    Collections.unmodifiableMap(nextRegions),
                    regionProxies,
                    Collections.unmodifiableSet(nextUnallocated));
        }

        public State withShardHomeDeallocated(String shardId) {
            ActorRef owner = shards.get(shardId);
            Map<String, ActorRef> nextShards = new HashMap<>(shards);
            nextShards.remove(shardId);

            Map<ActorRef, List<String>> nextRegions = new HashMap<>(regions);
            if (owner != null && nextRegions.containsKey(owner)) {
                List<String> updated = new ArrayList<>(nextRegions.get(owner));
                updated.remove(shardId);
                nextRegions.put(owner, Collections.unmodifiableList(updated));
            }
            Set<String> nextUnallocated = new HashSet<>(unallocatedShards);
            nextUnallocated.add(shardId);
            return new State(
                    Collections.unmodifiableMap(nextShards),
                    Collections.unmodifiableMap(nextRegions),
                    regionProxies,
                    Collections.unmodifiableSet(nextUnallocated));
        }

        public State withRegionTerminated(ActorRef region) {
            Map<ActorRef, List<String>> nextRegions = new HashMap<>(regions);
            List<String> lost = nextRegions.remove(region);
            Set<ActorRef> nextProxies = new HashSet<>(regionProxies);
            nextProxies.remove(region);

            Map<String, ActorRef> nextShards = new HashMap<>(shards);
            Set<String> nextUnallocated = new HashSet<>(unallocatedShards);
            if (lost != null) {
                for (String s : lost) {
                    nextShards.remove(s);
                    nextUnallocated.add(s);
                }
            }
            return new State(
                    Collections.unmodifiableMap(nextShards),
                    Collections.unmodifiableMap(nextRegions),
                    Collections.unmodifiableSet(nextProxies),
                    Collections.unmodifiableSet(nextUnallocated));
        }
    }

    protected enum RebalanceTick implements DeadLetterSuppression {
        INSTANCE
    }

    private record AllocateResult(String shardId, ActorRef region, Throwable error) {
    }

    private record RebalanceResult(Set<String> shards, Throwable error) {
    }

    /**
     * INTERNAL STABLE API：创建基于 DData 的 {@link DDataShardCoordinator} {@link Props}。
     */
    @InternalStableApi
    public static Props props(
            String typeName,
            ClusterShardingSettings settings,
            ShardAllocationStrategy allocationStrategy,
            ActorRef replicator,
            int majorityMinCap,
            RememberEntitiesStore.Provider rememberEntitiesProvider) {
        return Props.create(DDataShardCoordinator.class, () -> new DDataShardCoordinator(
                typeName,
                settings,
                allocationStrategy,
                replicator,
                majorityMinCap,
                rememberEntitiesProvider)).withDeploy(Deploy.local);
    }

    protected final String typeName;
    protected final ClusterShardingSettings settings;
    protected final ShardAllocationStrategy allocationStrategy;

    protected State state = State.EMPTY;
    protected boolean stateInitialized = false;
    protected final List<Object> stashedWhileInitializing = new ArrayList<>();
    protected final List<ActorRef> stashedSenders = new ArrayList<>();

    private final Map<String, List<ActorRef>> allocateInProgress = new HashMap<>();
    private final Map<String, Set<ActorRef>> rebalanceInProgress = new HashMap<>();
    private final Set<String> handOffInProgress = new HashSet<>();
    protected final Set<String> unallocatedToRestart = new HashSet<>();
    private Cancellable rebalanceTask;

    protected ShardCoordinator(
            String typeName,
            ClusterShardingSettings settings,
            ShardAllocationStrategy allocationStrategy) {
        this.typeName = typeName;
        this.settings = settings;
        this.allocationStrategy = allocationStrategy;
    }

    @Override
    public void preStart() {
        allocationStrategy.start(getContext().getSystem());
        Duration rebalanceInterval = settings.tuningParameters().rebalanceInterval();
        rebalanceTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                rebalanceInterval,
                rebalanceInterval,
                getSelf(),
                RebalanceTick.INSTANCE,
                getContext().getDispatcher(),
                getSelf());
    }

    @Override
    public void postStop() {
        if (rebalanceTask != null) {
            rebalanceTask.cancel();
        }
    }

    protected final void onStateInitialized(State loaded) {
        this.state = loaded;
        this.stateInitialized = true;
        for (ActorRef region : state.regions().keySet()) {
            getContext().watch(region);
        }
        for (ActorRef proxy : state.regionProxies()) {
            getContext().watch(proxy);
        }
        unallocatedToRestart.addAll(state.unallocatedShards());
        for (int i = 0; i < stashedWhileInitializing.size(); i++) {
            getSelf().tell(stashedWhileInitializing.get(i), stashedSenders.get(i));
        }
        stashedWhileInitializing.clear();
        stashedSenders.clear();
        restartRememberedShards();
    }

    protected final void stashOrHandleActive(Object msg) {
        if (!stateInitialized) {
            stashedWhileInitializing.add(msg);
            stashedSenders.add(getSender());
        } else {
            handleActive(msg);
        }
    }

    protected boolean handleActive(Object msg) {
        switch (msg) {
            case Internal.Register reg -> handleRegister(reg.shardRegion());
            case Internal.RegisterProxy proxy -> handleRegisterProxy(proxy.shardRegionProxy());
            case Internal.GetShardHome gsh -> handleGetShardHome(gsh.shard(), getSender());
            case AllocateResult ar -> handleAllocateResult(ar);
            case Internal.ShardStarted ignored -> { }
            case RebalanceTick ignored -> handleRebalanceTick();
            case RebalanceResult rr -> handleRebalanceResult(rr);
            case Internal.BeginHandOffAck ack -> handleBeginHandOffAck(ack.shard(), getSender());
            case Internal.ShardStopped stopped -> handleShardStopped(stopped.shard());
            case Terminated t -> handleTerminated(t.actor());
            default -> {
                return false;
            }
        }
        return true;
    }

    private void handleRegister(ActorRef region) {
        if (!state.regions().containsKey(region)) {
            getContext().watch(region);
            state = state.withRegionRegistered(region);
            persistState();
        }
        region.tell(new Internal.RegisterAck(getSelf()), getSelf());
        if (!state.regions().isEmpty()) {
            region.tell(new Internal.ShardHomes(state.regions()), getSelf());
        }
        restartRememberedShards();
    }

    private void handleRegisterProxy(ActorRef proxy) {
        if (!state.regionProxies().contains(proxy)) {
            getContext().watch(proxy);
            state = state.withProxyRegistered(proxy);
            persistState();
        }
        proxy.tell(new Internal.RegisterAck(getSelf()), getSelf());
        if (!state.regions().isEmpty()) {
            proxy.tell(new Internal.ShardHomes(state.regions()), getSelf());
        }
    }

    private void handleGetShardHome(String shardId, ActorRef requester) {
        if (rebalanceInProgress.containsKey(shardId) || handOffInProgress.contains(shardId)) {
            return;
        }
        ActorRef home = state.shards().get(shardId);
        if (home != null) {
            requester.tell(new Internal.ShardHome(shardId, home), getSelf());
            return;
        }
        if (state.regions().isEmpty()) {
            return;
        }
        List<ActorRef> waiting = allocateInProgress.get(shardId);
        if (waiting != null) {
            waiting.add(requester);
            return;
        }
        List<ActorRef> requesters = new ArrayList<>();
        requesters.add(requester);
        allocateInProgress.put(shardId, requesters);

        ActorRef self = getSelf();
        CompletionStage<ActorRef> future = allocationStrategy.allocateShard(requester, shardId, state.regions());
        future.whenComplete((region, err) -> self.tell(new AllocateResult(shardId, region, err), ActorRef.noSender));
    }

    private void handleAllocateResult(AllocateResult ar) {
        List<ActorRef> waiters = allocateInProgress.remove(ar.shardId());
        if (ar.error() != null || ar.region() == null) {
            log().warning("{}: Failed to allocate shard [{}]: {}", typeName, ar.shardId(),
                    ar.error() == null ? "null region" : ar.error().getMessage());
            return;
        }
        if (!state.regions().containsKey(ar.region())) {
            return;
        }
        state = state.withShardHomeAllocated(ar.shardId(), ar.region());
        persistState();
        onShardAllocated(ar.shardId());
        ar.region().tell(new Internal.HostShard(ar.shardId()), getSelf());
        if (waiters != null) {
            Internal.ShardHome homeMsg = new Internal.ShardHome(ar.shardId(), ar.region());
            for (ActorRef waiter : waiters) {
                waiter.tell(homeMsg, getSelf());
            }
        }
    }

    private void handleRebalanceTick() {
        if (state.regions().size() <= 1) {
            return;
        }
        Set<String> inProgress = new HashSet<>(rebalanceInProgress.keySet());
        inProgress.addAll(handOffInProgress);
        ActorRef self = getSelf();
        allocationStrategy.rebalance(state.regions(), inProgress)
                .whenComplete((shards, err) -> self.tell(new RebalanceResult(shards, err), ActorRef.noSender));
    }

    private void handleRebalanceResult(RebalanceResult rr) {
        if (rr.error() != null || rr.shards() == null || rr.shards().isEmpty()) {
            return;
        }
        for (String shardId : rr.shards()) {
            if (!state.shards().containsKey(shardId) || rebalanceInProgress.containsKey(shardId)) {
                continue;
            }
            Set<ActorRef> pendingAcks = new HashSet<>(state.regions().keySet());
            pendingAcks.addAll(state.regionProxies());
            if (pendingAcks.isEmpty()) {
                continue;
            }
            rebalanceInProgress.put(shardId, pendingAcks);
            Internal.BeginHandOff beginMsg = new Internal.BeginHandOff(shardId);
            for (ActorRef target : pendingAcks) {
                target.tell(beginMsg, getSelf());
            }
        }
    }

    private void handleBeginHandOffAck(String shardId, ActorRef sender) {
        Set<ActorRef> pending = rebalanceInProgress.get(shardId);
        if (pending == null) {
            return;
        }
        pending.remove(sender);
        if (pending.isEmpty()) {
            rebalanceInProgress.remove(shardId);
            ActorRef owner = state.shards().get(shardId);
            if (owner != null) {
                handOffInProgress.add(shardId);
                owner.tell(new Internal.HandOff(shardId), getSelf());
            }
        }
    }

    private void handleShardStopped(String shardId) {
        handOffInProgress.remove(shardId);
        state = state.withShardHomeDeallocated(shardId);
        persistState();
        if (settings.rememberEntities() && !state.regions().isEmpty()) {
            unallocatedToRestart.add(shardId);
            restartRememberedShards();
        }
    }

    private void handleTerminated(ActorRef region) {
        boolean wasRegionOrProxy = state.regions().containsKey(region) || state.regionProxies().contains(region);
        if (!wasRegionOrProxy) {
            return;
        }
        for (Set<ActorRef> pending : rebalanceInProgress.values()) {
            pending.remove(region);
        }
        state = state.withRegionTerminated(region);
        persistState();
        if (settings.rememberEntities()) {
            unallocatedToRestart.addAll(state.unallocatedShards());
            restartRememberedShards();
        }
    }

    protected final void restartRememberedShards() {
        if (state.regions().isEmpty() || unallocatedToRestart.isEmpty()) {
            return;
        }
        for (String shardId : List.copyOf(unallocatedToRestart)) {
            if (state.shards().containsKey(shardId)) {
                unallocatedToRestart.remove(shardId);
                continue;
            }
            if (!allocateInProgress.containsKey(shardId)) {
                unallocatedToRestart.remove(shardId);
                handleGetShardHome(shardId, getSelf());
            }
        }
    }

    protected void onShardAllocated(String shardId) {
    }

    protected abstract void persistState();
}
