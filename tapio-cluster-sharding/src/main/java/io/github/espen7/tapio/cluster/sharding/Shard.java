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
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.sharding.internal.EntityPassivationStrategy;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * INTERNAL API：管理单个分片内所有实体 Actor 生命周期的容器 Actor。
 */
@InternalApi
public final class Shard extends AbstractLoggingActor {

    /** 查询分片统计消息。 */
    public enum GetShardStats {
        INSTANCE
    }

    /** 分片统计响应。 */
    public record ShardStats(String shardId, int entityCount) implements ClusterShardingSerializable {
    }

    /** 查询当前分片状态消息。 */
    public enum GetCurrentShardState {
        INSTANCE
    }

    /** 分片当前状态响应。 */
    public record CurrentShardState(String shardId, Set<String> entityIds) {
    }

    private enum PassivateIdleTick {
        INSTANCE
    }

    private record BufferedMessage(Object message, ActorRef sender) {
    }

    public static Props props(
            String typeName,
            String shardId,
            Function<String, Props> entityProps,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor,
            Object handOffStopMessage,
            Props rememberEntitiesStoreProps) {
        return Props.create(Shard.class, () -> new Shard(
                typeName,
                shardId,
                entityProps,
                settings,
                messageExtractor,
                handOffStopMessage,
                rememberEntitiesStoreProps));
    }

    private final String typeName;
    private final String shardId;
    private final Function<String, Props> entityProps;
    private final ClusterShardingSettings settings;
    private final ShardRegion.MessageExtractor messageExtractor;
    private final Object handOffStopMessage;
    private final ActorRef rememberEntitiesStore;

    private final Map<ActorRef, String> idByRef = new HashMap<>();
    private final Map<String, ActorRef> refById = new HashMap<>();
    private final EntityPassivationStrategy passivationStrategy;
    private final Map<String, List<BufferedMessage>> messageBuffers = new HashMap<>();
    private final Set<ActorRef> passivating = new HashSet<>();

    private boolean handingOff = false;
    private Cancellable idleCheckTask;

    public Shard(
            String typeName,
            String shardId,
            Function<String, Props> entityProps,
            ClusterShardingSettings settings,
            ShardRegion.MessageExtractor messageExtractor,
            Object handOffStopMessage,
            Props rememberEntitiesStoreProps) {
        this.typeName = typeName;
        this.shardId = shardId;
        this.entityProps = entityProps;
        this.settings = settings;
        this.messageExtractor = messageExtractor;
        this.handOffStopMessage = handOffStopMessage;
        this.rememberEntitiesStore = rememberEntitiesStoreProps == null
                ? null
                : getContext().actorOf(rememberEntitiesStoreProps, "rememberEntitiesStore");
        this.passivationStrategy = EntityPassivationStrategy.create(settings);
    }

    @Override
    public void preStart() {
        if (rememberEntitiesStore != null) {
            rememberEntitiesStore.tell(RememberEntitiesStore.ShardStore.GetEntities.INSTANCE, getSelf());
        }
        Duration checkInterval = passivationStrategy.interval();
        if (!checkInterval.isZero() && !checkInterval.isNegative()) {
            idleCheckTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                    checkInterval,
                    checkInterval,
                    getSelf(),
                    PassivateIdleTick.INSTANCE,
                    getContext().getDispatcher(),
                    getSelf());
        }
    }

    @Override
    public void postStop() {
        if (idleCheckTask != null) {
            idleCheckTask.cancel();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Terminated.class, this::receiveTerminated)
                .match(ShardRegion.Passivate.class, p -> passivate(getSender(), p.stopMessage()))
                .match(ShardCoordinator.Internal.HandOff.class, this::receiveHandOff)
                .match(GetShardStats.class, msg -> getSender().tell(new ShardStats(shardId, refById.size()), getSelf()))
                .match(GetCurrentShardState.class,
                        msg -> getSender().tell(new CurrentShardState(shardId, Set.copyOf(refById.keySet())), getSelf()))
                .match(RememberEntitiesStore.ShardStore.RememberedEntities.class, this::receiveRememberedEntities)
                .match(RememberEntitiesStore.ShardStore.UpdateDone.class, msg -> { })
                .match(PassivateIdleTick.class, msg -> checkIdleEntities())
                .match(ShardRegion.StartEntity.class, se -> {
                    getOrCreateEntity(se.entityId());
                    getSender().tell(new ShardRegion.StartEntityAck(se.entityId(), shardId), getSelf());
                })
                .matchAny(this::deliverMessage)
                .build();
    }

    private void receiveRememberedEntities(RememberEntitiesStore.ShardStore.RememberedEntities remembered) {
        if (!remembered.entities().isEmpty()) {
            log().debug("{}: Restarting {} remembered entities in shard [{}]",
                    typeName, remembered.entities().size(), shardId);
            for (String entityId : remembered.entities()) {
                getOrCreateEntity(entityId);
            }
        }
    }

    private void deliverMessage(Object msg) {
        String entityId = messageExtractor.entityId(msg);
        if (entityId == null || entityId.isEmpty()) {
            unhandled(msg);
            return;
        }
        Object payload = messageExtractor.entityMessage(msg);
        if (handingOff) {
            getContext().getParent().tell(new ShardRegion.HandOffBufferedMessage(shardId, msg, getSender()), getSelf());
            return;
        }
        ActorRef existing = refById.get(entityId);
        if (existing != null && passivating.contains(existing)) {
            messageBuffers.computeIfAbsent(entityId, k -> new ArrayList<>())
                    .add(new BufferedMessage(payload, getSender()));
            return;
        }
        ActorRef entity = getOrCreateEntity(entityId);
        touchEntity(entityId);
        entity.tell(payload, getSender());
    }

    private ActorRef getOrCreateEntity(String entityId) {
        ActorRef existing = refById.get(entityId);
        if (existing != null) {
            return existing;
        }
        String childName = URLEncoder.encode(entityId, StandardCharsets.UTF_8);
        ActorRef created = getContext().actorOf(entityProps.apply(entityId), childName);
        getContext().watch(created);
        refById.put(entityId, created);
        idByRef.put(created, entityId);
        touchEntity(entityId);
        if (rememberEntitiesStore != null) {
            rememberEntitiesStore.tell(
                    new RememberEntitiesStore.ShardStore.Update(Set.of(entityId), Set.of()), getSelf());
        }
        return created;
    }

    private void touchEntity(String entityId) {
        for (String toPassivate : passivationStrategy.entityTouched(entityId)) {
            ActorRef ref = refById.get(toPassivate);
            if (ref != null) {
                passivate(ref, handOffStopMessage);
            }
        }
    }

    private void passivate(ActorRef entity, Object stopMessage) {
        String entityId = idByRef.get(entity);
        if (entityId == null) {
            return;
        }
        if (passivating.add(entity)) {
            entity.tell(stopMessage, getSelf());
        }
    }

    private void checkIdleEntities() {
        if (handingOff || refById.isEmpty()) {
            return;
        }
        for (String entityId : passivationStrategy.checkIdle()) {
            ActorRef ref = refById.get(entityId);
            if (ref != null && !passivating.contains(ref)) {
                passivate(ref, handOffStopMessage);
            }
        }
    }

    private void receiveHandOff(ShardCoordinator.Internal.HandOff msg) {
        if (handingOff) {
            return;
        }
        handingOff = true;
        if (refById.isEmpty()) {
            getContext().stop(getSelf());
            return;
        }
        for (ActorRef entity : List.copyOf(refById.values())) {
            entity.tell(handOffStopMessage, getSelf());
        }
    }

    private void receiveTerminated(Terminated t) {
        ActorRef ref = t.actor();
        String entityId = idByRef.remove(ref);
        if (entityId == null) {
            return;
        }
        refById.remove(entityId);
        passivationStrategy.entityTerminated(entityId);
        boolean wasPassivating = passivating.remove(ref);
        if (!handingOff && wasPassivating && rememberEntitiesStore != null) {
            rememberEntitiesStore.tell(
                    new RememberEntitiesStore.ShardStore.Update(Set.of(), Set.of(entityId)), getSelf());
        }
        List<BufferedMessage> buffered = messageBuffers.remove(entityId);
        if (buffered != null && !buffered.isEmpty() && !handingOff) {
            ActorRef entity = getOrCreateEntity(entityId);
            for (BufferedMessage bm : buffered) {
                touchEntity(entityId);
                entity.tell(bm.message(), bm.sender());
            }
        }
        if (handingOff && idByRef.isEmpty()) {
            getContext().stop(getSelf());
        }
    }
}
