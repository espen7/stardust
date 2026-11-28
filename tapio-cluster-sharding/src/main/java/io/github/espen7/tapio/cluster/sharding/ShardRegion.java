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
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.cluster.sharding.internal.RememberEntitiesStore;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * 分片区域 Actor，负责在集群节点间按分片路由消息与托管本地 {@link Shard}。
 */
public final class ShardRegion extends AbstractLoggingActor {

    /**
     * 从业务消息提取实体 ID、实体消息与分片 ID 的接口。
     */
    public interface MessageExtractor {
        /**
         * 提取目标实体标识。
         *
         * @param message 入站消息
         * @return 实体 ID，若不匹配则返回 {@code null}
         */
        String entityId(Object message);

        /**
         * 提取转发给实体 Actor 的实际消息载荷。
         *
         * @param message 入站消息
         * @return 实体消息
         */
        Object entityMessage(Object message);

        /**
         * 提取分片标识。
         *
         * @param message 入站消息
         * @return 分片 ID，若不匹配则返回 {@code null}
         */
        String shardId(Object message);
    }

    /**
     * 基于实体 ID 哈希取模的 {@link MessageExtractor} 抽象基类。
     */
    public abstract static class HashCodeMessageExtractor implements MessageExtractor {
        private final int maxNumberOfShards;

        protected HashCodeMessageExtractor(int maxNumberOfShards) {
            if (maxNumberOfShards <= 0) {
                throw new IllegalArgumentException("maxNumberOfShards must be > 0");
            }
            this.maxNumberOfShards = maxNumberOfShards;
        }

        @Override
        public Object entityMessage(Object message) {
            return message;
        }

        @Override
        public String shardId(Object message) {
            if (message instanceof StartEntity se) {
                return String.valueOf(Math.abs(se.entityId().hashCode()) % maxNumberOfShards);
            }
            String id = entityId(message);
            if (id == null) {
                return null;
            }
            return String.valueOf(Math.abs(id.hashCode()) % maxNumberOfShards);
        }
    }

    /**
     * 实体向所属 {@link Shard} 发送的钝化请求。
     */
    public record Passivate(Object stopMessage) implements Serializable {
    }

    /**
     * 显式启动指定实体（即使尚未发送业务消息）。
     */
    public record StartEntity(String entityId) implements ClusterShardingSerializable {
    }

    /**
     * {@link StartEntity} 的确认响应。
     */
    public record StartEntityAck(String entityId, String shardId) implements ClusterShardingSerializable {
    }

    /** 触发优雅下线：向协调器交还所有分片。 */
    public enum GracefulShutdown {
        INSTANCE
    }

    /** 查询本地 ShardRegion 统计。 */
    public enum GetShardRegionStats implements ClusterShardingSerializable {
        INSTANCE
    }

    /** 本地 ShardRegion 统计响应。 */
    public record ShardRegionStats(Map<String, Integer> stats, Set<String> failed)
            implements ClusterShardingSerializable {
    }

    /** 查询整个集群各 ShardRegion 的统计信息。 */
    public record GetClusterShardingStats(Duration timeout) implements ClusterShardingSerializable {
    }

    /** 全集群分片统计响应。 */
    public record ClusterShardingStats(Map<Address, ShardRegionStats> regions)
            implements ClusterShardingSerializable {
    }

    /** 查询当前所有注册 region 的节点地址。 */
    public enum GetCurrentRegions implements ClusterShardingSerializable {
        INSTANCE
    }

    /** 当前所有注册 region 的节点地址响应。 */
    public record CurrentRegions(Set<Address> regions) implements ClusterShardingSerializable {
    }

    /** 查询本地 ShardRegion 状态。 */
    public enum GetShardRegionState implements ClusterShardingSerializable {
        INSTANCE
    }

    /** 单个分片与其实体 ID 集合。 */
    public record ShardState(String shardId, Set<String> entityIds) {
    }

    /** 本地 ShardRegion 状态响应。 */
    public record CurrentShardRegionState(Set<ShardState> shards) {
    }

    /** INTERNAL API：Shard 在 HandOff 期间收到的消息回退给 Region 重缓冲。 */
    @InternalApi
    record HandOffBufferedMessage(String shardId, Object message, ActorRef sender) {
    }

    private enum RegisterTick implements DeadLetterSuppression {
        INSTANCE
    }

    private enum RetryTick implements DeadLetterSuppression {
        INSTANCE
    }

    private record BufferedMessage(Object message, ActorRef sender) {
    }

    @InternalApi
    public static Props props(
            String typeName,
            Function<String, Props> entityProps,
            ClusterShardingSettings settings,
            String coordinatorPath,
            MessageExtractor messageExtractor,
            Object handOffStopMessage,
            RememberEntitiesStore.Provider rememberEntitiesProvider) {
        return Props.create(ShardRegion.class, () -> new ShardRegion(
                typeName,
                entityProps,
                settings,
                coordinatorPath,
                messageExtractor,
                handOffStopMessage,
                rememberEntitiesProvider)).withDeploy(Deploy.local);
    }

    @InternalApi
    public static Props proxyProps(
            String typeName,
            ClusterShardingSettings settings,
            String coordinatorPath,
            MessageExtractor messageExtractor) {
        return Props.create(ShardRegion.class, () -> new ShardRegion(
                typeName,
                null,
                settings,
                coordinatorPath,
                messageExtractor,
                null,
                null)).withDeploy(Deploy.local);
    }

    private static final Comparator<Member> AGE_ORDERING = (a, b) -> {
        if (a.isOlderThan(b)) {
            return -1;
        }
        if (b.isOlderThan(a)) {
            return 1;
        }
        return Member.ADDRESS_ORDERING.compare(a.address(), b.address());
    };

    private final String typeName;
    private final Function<String, Props> entityProps;
    private final ClusterShardingSettings settings;
    private final String coordinatorPath;
    private final MessageExtractor messageExtractor;
    private final Object handOffStopMessage;
    private final RememberEntitiesStore.Provider rememberEntitiesProvider;
    private final Cluster cluster;

    private final NavigableSet<Member> membersByAge = new TreeSet<>(AGE_ORDERING);
    private final Map<ActorRef, Set<String>> regions = new HashMap<>();
    private final Map<String, ActorRef> regionByShard = new HashMap<>();
    private final Map<String, List<BufferedMessage>> shardBuffers = new HashMap<>();
    private final Map<String, ActorRef> shards = new HashMap<>();
    private final Map<ActorRef, String> shardsByRef = new HashMap<>();
    private final Set<ActorRef> handingOff = new HashSet<>();

    private ActorRef coordinator;
    private Cancellable retryTask;
    private boolean gracefulShutdownInProgress = false;
    private int totalBufferSize = 0;

    public ShardRegion(
            String typeName,
            Function<String, Props> entityProps,
            ClusterShardingSettings settings,
            String coordinatorPath,
            MessageExtractor messageExtractor,
            Object handOffStopMessage,
            RememberEntitiesStore.Provider rememberEntitiesProvider) {
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.entityProps = entityProps;
        this.settings = Objects.requireNonNull(settings, "settings");
        this.coordinatorPath = Objects.requireNonNull(coordinatorPath, "coordinatorPath");
        this.messageExtractor = Objects.requireNonNull(messageExtractor, "messageExtractor");
        this.handOffStopMessage = handOffStopMessage;
        this.rememberEntitiesProvider = rememberEntitiesProvider;
        this.cluster = Cluster.get(getContext().getSystem());
    }

    private boolean isProxyOnly() {
        return entityProps == null;
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.MemberEvent.class);
        Duration retryInterval = settings.tuningParameters().retryInterval();
        retryTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                retryInterval,
                retryInterval,
                getSelf(),
                RetryTick.INSTANCE,
                getContext().getDispatcher(),
                getSelf());
    }

    @Override
    public void postStop() {
        if (retryTask != null) {
            retryTask.cancel();
        }
        cluster.unsubscribe(getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.CurrentClusterState.class, this::receiveInitialState)
                .match(ClusterEvent.MemberUp.class, m -> addMember(m.member()))
                .match(ClusterEvent.MemberRemoved.class, m -> removeMember(m.member()))
                .match(ClusterEvent.MemberEvent.class, ignored -> { })
                .match(RegisterTick.class, ignored -> register())
                .match(RetryTick.class, ignored -> onRetryTick())
                .match(ShardCoordinator.Internal.RegisterAck.class, this::receiveRegisterAck)
                .match(ShardCoordinator.Internal.ShardHome.class, this::receiveShardHome)
                .match(ShardCoordinator.Internal.ShardHomes.class, this::receiveShardHomes)
                .match(ShardCoordinator.Internal.HostShard.class, this::receiveHostShard)
                .match(ShardCoordinator.Internal.BeginHandOff.class, this::receiveBeginHandOff)
                .match(ShardCoordinator.Internal.HandOff.class, this::receiveHandOff)
                .match(Terminated.class, this::receiveTerminated)
                .match(GracefulShutdown.class, ignored -> startGracefulShutdown())
                .match(GetCurrentRegions.class, ignored -> handleGetCurrentRegions())
                .match(GetShardRegionStats.class, ignored -> handleGetShardRegionStats())
                .match(GetClusterShardingStats.class, msg -> {
                    if (coordinator != null) {
                        coordinator.forward(msg, getContext());
                    } else {
                        getSender().tell(new ClusterShardingStats(Map.of()), getSelf());
                    }
                })
                .match(HandOffBufferedMessage.class, this::rebufferHandOffMessage)
                .matchAny(this::deliverMessage)
                .build();
    }

    private void receiveInitialState(ClusterEvent.CurrentClusterState state) {
        membersByAge.clear();
        for (Member m : state.members()) {
            if (isEligibleMember(m)) {
                membersByAge.add(m);
            }
        }
        if (coordinator == null) {
            register();
        }
    }

    private void addMember(Member m) {
        if (isEligibleMember(m)) {
            membersByAge.add(m);
            if (coordinator == null) {
                register();
            }
        }
    }

    private void removeMember(Member m) {
        membersByAge.remove(m);
        for (ActorRef region : List.copyOf(regions.keySet())) {
            Address addr = region.path().address();
            if (addr.equals(m.address())) {
                dropRegion(region);
            }
        }
        if (coordinator != null && coordinator.path().address().equals(m.address())) {
            coordinator = null;
            register();
        }
    }

    private boolean isEligibleMember(Member m) {
        if (m.status() != MemberStatus.Up && m.status() != MemberStatus.WeaklyUp) {
            return false;
        }
        return settings.role().map(m::hasRole).orElse(true);
    }

    private Optional<ActorRef> coordinatorSelectionTarget() {
        return Optional.empty();
    }

    private void register() {
        if (coordinator != null || membersByAge.isEmpty()) {
            return;
        }
        Member oldest = membersByAge.first();
        Object regMsg = isProxyOnly()
                ? new ShardCoordinator.Internal.RegisterProxy(getSelf())
                : new ShardCoordinator.Internal.Register(getSelf());
        Address targetAddr = oldest.address();
        String fullPath = targetAddr.equals(cluster.selfAddress())
                ? coordinatorPath
                : targetAddr + coordinatorPath;
        getContext().actorSelection(fullPath).tell(regMsg, getSelf());
    }

    private void onRetryTick() {
        if (coordinator == null) {
            register();
        } else if (!shardBuffers.isEmpty()) {
            for (String shardId : List.copyOf(shardBuffers.keySet())) {
                if (!regionByShard.containsKey(shardId)) {
                    coordinator.tell(new ShardCoordinator.Internal.GetShardHome(shardId), getSelf());
                }
            }
        }
    }

    private void receiveRegisterAck(ShardCoordinator.Internal.RegisterAck ack) {
        if (coordinator != null && coordinator.equals(ack.coordinator())) {
            return;
        }
        coordinator = ack.coordinator();
        getContext().watch(coordinator);
        for (String shardId : List.copyOf(shardBuffers.keySet())) {
            if (!regionByShard.containsKey(shardId)) {
                coordinator.tell(new ShardCoordinator.Internal.GetShardHome(shardId), getSelf());
            }
        }
    }

    private void receiveShardHomes(ShardCoordinator.Internal.ShardHomes homes) {
        for (Map.Entry<ActorRef, List<String>> entry : homes.homes().entrySet()) {
            ActorRef region = entry.getKey();
            for (String shardId : entry.getValue()) {
                assignShardHome(shardId, region);
            }
        }
    }

    private void receiveShardHome(ShardCoordinator.Internal.ShardHome home) {
        assignShardHome(home.shard(), home.ref());
    }

    private void assignShardHome(String shardId, ActorRef region) {
        ActorRef previous = regionByShard.put(shardId, region);
        if (previous != null && !previous.equals(region)) {
            Set<String> prevShards = regions.get(previous);
            if (prevShards != null) {
                prevShards.remove(shardId);
            }
        }
        regions.computeIfAbsent(region, k -> {
            getContext().watch(region);
            return new HashSet<>();
        }).add(shardId);
        deliverBufferedMessages(shardId, region);
    }

    private void receiveHostShard(ShardCoordinator.Internal.HostShard msg) {
        String shardId = msg.shard();
        if (!isProxyOnly()) {
            getOrCreateShard(shardId);
            assignShardHome(shardId, getSelf());
        }
        getSender().tell(new ShardCoordinator.Internal.ShardStarted(shardId), getSelf());
    }

    private ActorRef getOrCreateShard(String shardId) {
        ActorRef existing = shards.get(shardId);
        if (existing != null) {
            return existing;
        }
        String childName = URLEncoder.encode(shardId, StandardCharsets.UTF_8);
        Props storeProps = rememberEntitiesProvider == null
                ? null
                : rememberEntitiesProvider.shardStoreProps(shardId);
        ActorRef shard = getContext().actorOf(
                Shard.props(typeName, shardId, entityProps, settings, messageExtractor, handOffStopMessage, storeProps),
                childName);
        getContext().watch(shard);
        shards.put(shardId, shard);
        shardsByRef.put(shard, shardId);
        return shard;
    }

    private void receiveBeginHandOff(ShardCoordinator.Internal.BeginHandOff msg) {
        String shardId = msg.shard();
        ActorRef currentRegion = regionByShard.remove(shardId);
        if (currentRegion != null) {
            Set<String> set = regions.get(currentRegion);
            if (set != null) {
                set.remove(shardId);
            }
        }
        getSender().tell(new ShardCoordinator.Internal.BeginHandOffAck(shardId), getSelf());
    }

    private void receiveHandOff(ShardCoordinator.Internal.HandOff msg) {
        String shardId = msg.shard();
        ActorRef shard = shards.get(shardId);
        if (shard != null) {
            handingOff.add(shard);
            shard.tell(msg, getSelf());
        } else {
            if (coordinator != null) {
                coordinator.tell(new ShardCoordinator.Internal.ShardStopped(shardId), getSelf());
            }
        }
    }

    private void receiveTerminated(Terminated t) {
        ActorRef ref = t.actor();
        if (ref.equals(coordinator)) {
            coordinator = null;
            register();
            return;
        }
        String shardId = shardsByRef.remove(ref);
        if (shardId != null) {
            shards.remove(shardId);
            if (handingOff.remove(ref)) {
                if (coordinator != null) {
                    coordinator.tell(new ShardCoordinator.Internal.ShardStopped(shardId), getSelf());
                }
            }
            if (gracefulShutdownInProgress && shards.isEmpty()) {
                getContext().stop(getSelf());
            }
            return;
        }
        dropRegion(ref);
    }

    private void dropRegion(ActorRef region) {
        Set<String> lostShards = regions.remove(region);
        if (lostShards != null) {
            for (String shardId : lostShards) {
                regionByShard.remove(shardId, region);
            }
        }
    }

    private void startGracefulShutdown() {
        gracefulShutdownInProgress = true;
        if (shards.isEmpty()) {
            getContext().stop(getSelf());
            return;
        }
        for (ActorRef shard : List.copyOf(shards.values())) {
            handingOff.add(shard);
            shard.tell(new ShardCoordinator.Internal.HandOff(shardsByRef.get(shard)), getSelf());
        }
    }

    private void handleGetCurrentRegions() {
        Set<Address> addrs = new HashSet<>();
        for (ActorRef r : regions.keySet()) {
            Address addr = r.path().address();
            addrs.add(addr.host() == null ? cluster.selfAddress() : addr);
        }
        getSender().tell(new CurrentRegions(addrs), getSelf());
    }

    private void handleGetShardRegionStats() {
        Map<String, Integer> stats = new HashMap<>();
        for (String shardId : shards.keySet()) {
            stats.put(shardId, 1);
        }
        getSender().tell(new ShardRegionStats(stats, Set.of()), getSelf());
    }

    private void rebufferHandOffMessage(HandOffBufferedMessage msg) {
        bufferMessage(msg.shardId(), msg.message(), msg.sender());
    }

    private void deliverMessage(Object msg) {
        String shardId = messageExtractor.shardId(msg);
        if (shardId == null || shardId.isEmpty()) {
            unhandled(msg);
            return;
        }
        ActorRef region = regionByShard.get(shardId);
        if (region == null) {
            bufferMessage(shardId, msg, getSender());
            if (coordinator != null) {
                coordinator.tell(new ShardCoordinator.Internal.GetShardHome(shardId), getSelf());
            } else {
                register();
            }
            return;
        }
        if (region.equals(getSelf()) && !isProxyOnly()) {
            ActorRef shard = shards.get(shardId);
            if (shard == null) {
                shard = getOrCreateShard(shardId);
            }
            if (handingOff.contains(shard)) {
                bufferMessage(shardId, msg, getSender());
            } else {
                shard.tell(msg, getSender());
            }
        } else {
            region.tell(msg, getSender());
        }
    }

    private void bufferMessage(String shardId, Object msg, ActorRef sender) {
        int maxBuffer = settings.tuningParameters().bufferSize();
        if (totalBufferSize >= maxBuffer) {
            log().warning("{}: Dropping message for shard [{}] because buffer size [{}] is full",
                    typeName, shardId, maxBuffer);
            return;
        }
        shardBuffers.computeIfAbsent(shardId, k -> new ArrayList<>()).add(new BufferedMessage(msg, sender));
        totalBufferSize++;
    }

    private void deliverBufferedMessages(String shardId, ActorRef region) {
        List<BufferedMessage> buffered = shardBuffers.remove(shardId);
        if (buffered == null || buffered.isEmpty()) {
            return;
        }
        totalBufferSize = Math.max(0, totalBufferSize - buffered.size());
        for (BufferedMessage bm : buffered) {
            if (region.equals(getSelf()) && !isProxyOnly()) {
                ActorRef shard = getOrCreateShard(shardId);
                shard.tell(bm.message(), bm.sender());
            } else {
                region.tell(bm.message(), bm.sender());
            }
        }
    }
}
