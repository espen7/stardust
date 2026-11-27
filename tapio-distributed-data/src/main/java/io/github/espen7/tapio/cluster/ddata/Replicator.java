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

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.cluster.UniqueAddress;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * 分布式状态型 CRDT 复制器 Actor，提供 {@link Update}、{@link Get}、{@link Subscribe}、{@link Delete} 与基于 Gossip 的最终一致性复制。
 */
public final class Replicator extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    /** 写一致性级别。 */
    public sealed interface WriteConsistency permits WriteLocal, WriteTo, WriteMajority, WriteAll {
        Duration timeout();
    }

    /** 仅写入本地副本即返回成功。 */
    public static final class WriteLocal implements WriteConsistency {
        public static final WriteLocal INSTANCE = new WriteLocal();

        private WriteLocal() {
        }

        @Override
        public Duration timeout() {
            return Duration.ZERO;
        }
    }

    /** 至少写入 {@code n} 个节点。 */
    public record WriteTo(int n, Duration timeout) implements WriteConsistency {
    }

    /** 至少写入多数派节点。 */
    public record WriteMajority(Duration timeout, int minCap) implements WriteConsistency {
        public WriteMajority(Duration timeout) {
            this(timeout, 0);
        }
    }

    /** 写入全部活跃副本节点。 */
    public record WriteAll(Duration timeout) implements WriteConsistency {
    }

    /** 读一致性级别。 */
    public sealed interface ReadConsistency permits ReadLocal, ReadFrom, ReadMajority, ReadAll {
        Duration timeout();
    }

    /** 仅读取本地副本。 */
    public static final class ReadLocal implements ReadConsistency {
        public static final ReadLocal INSTANCE = new ReadLocal();

        private ReadLocal() {
        }

        @Override
        public Duration timeout() {
            return Duration.ZERO;
        }
    }

    /** 从至少 {@code n} 个节点读取并合并。 */
    public record ReadFrom(int n, Duration timeout) implements ReadConsistency {
    }

    /** 从多数派节点读取并合并。 */
    public record ReadMajority(Duration timeout, int minCap) implements ReadConsistency {
        public ReadMajority(Duration timeout) {
            this(timeout, 0);
        }
    }

    /** 从全部活跃副本节点读取并合并。 */
    public record ReadAll(Duration timeout) implements ReadConsistency {
    }

    /** 更新（或初始化）CRDT 条目请求。 */
    public static final class Update<A extends ReplicatedData> {
        private final Key<A> key;
        private final WriteConsistency consistency;
        private final Optional<Object> request;
        private final Function<Optional<A>, A> modify;

        public Update(Key<A> key, A initial, WriteConsistency consistency, Function<A, A> modify) {
            this(key, initial, consistency, Optional.empty(), modify);
        }

        public Update(
                Key<A> key,
                A initial,
                WriteConsistency consistency,
                Optional<Object> request,
                Function<A, A> modify) {
            this.key = key;
            this.consistency = consistency;
            this.request = request == null ? Optional.empty() : request;
            this.modify = current -> modify.apply(current.orElse(initial));
        }

        public Key<A> key() {
            return key;
        }

        public WriteConsistency consistency() {
            return consistency;
        }

        public Optional<Object> request() {
            return request;
        }

        @SuppressWarnings("unchecked")
        A applyModify(Optional<ReplicatedData> existing) {
            return modify.apply(existing.map(d -> (A) d));
        }
    }

    /** 更新响应基接口。 */
    public sealed interface UpdateResponse<A extends ReplicatedData>
            permits UpdateSuccess, ModifyFailure, UpdateTimeout {
        Key<A> key();

        Optional<Object> request();
    }

    /** 更新成功响应。 */
    public record UpdateSuccess<A extends ReplicatedData>(Key<A> key, Optional<Object> request)
            implements UpdateResponse<A> {
    }

    /** 修改函数抛出异常时的失败响应。 */
    public record ModifyFailure<A extends ReplicatedData>(
            Key<A> key, String errorMessage, Throwable cause, Optional<Object> request)
            implements UpdateResponse<A> {
    }

    /** 写一致性超时响应。 */
    public record UpdateTimeout<A extends ReplicatedData>(Key<A> key, Optional<Object> request)
            implements UpdateResponse<A> {
    }

    /** 读取 CRDT 条目请求。 */
    public record Get<A extends ReplicatedData>(
            Key<A> key, ReadConsistency consistency, Optional<Object> request) {
        public Get(Key<A> key, ReadConsistency consistency) {
            this(key, consistency, Optional.empty());
        }
    }

    /** 读取响应基接口。 */
    public sealed interface GetResponse<A extends ReplicatedData>
            permits GetSuccess, NotFound, GetFailure {
        Key<A> key();

        Optional<Object> request();
    }

    /** 读取成功响应。 */
    public record GetSuccess<A extends ReplicatedData>(Key<A> key, Optional<Object> request, A dataValue)
            implements GetResponse<A> {
        @SuppressWarnings("unchecked")
        public A get(Key<A> k) {
            return dataValue;
        }
    }

    /** 键不存在响应。 */
    public record NotFound<A extends ReplicatedData>(Key<A> key, Optional<Object> request)
            implements GetResponse<A> {
    }

    /** 读一致性超时响应。 */
    public record GetFailure<A extends ReplicatedData>(Key<A> key, Optional<Object> request)
            implements GetResponse<A> {
    }

    /** 订阅键变更通知。 */
    public record Subscribe<A extends ReplicatedData>(Key<A> key, ActorRef subscriber) {
    }

    /** 退订键变更通知。 */
    public record Unsubscribe<A extends ReplicatedData>(Key<A> key, ActorRef subscriber) {
    }

    /** 键数据变更通知。 */
    public record Changed<A extends ReplicatedData>(Key<A> key, A dataValue) {
        public A get(Key<A> k) {
            return dataValue;
        }
    }

    /** 删除键请求。 */
    public record Delete<A extends ReplicatedData>(
            Key<A> key, WriteConsistency consistency, Optional<Object> request) {
        public Delete(Key<A> key, WriteConsistency consistency) {
            this(key, consistency, Optional.empty());
        }
    }

    /** 删除成功响应。 */
    public record DeleteSuccess<A extends ReplicatedData>(Key<A> key, Optional<Object> request) {
    }

    /** 查询活跃副本数量请求。 */
    public static final class GetReplicaCount {
        public static final GetReplicaCount INSTANCE = new GetReplicaCount();

        private GetReplicaCount() {
        }
    }

    /** 活跃副本数量响应。 */
    public record ReplicaCount(int n) {
    }

    /** 内部复制协议消息（由 ReplicatorMessageSerializer 序列化）。 */
    public record DataEnvelope(ReplicatedData data) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** 内部直接写消息。 */
    public record Write(String keyId, DataEnvelope envelope, long correlationId)
            implements ReplicatorMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** 内部直接写确认。 */
    public record WriteAck(long correlationId) implements ReplicatorMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** 内部直接读消息。 */
    public record Read(String keyId, long correlationId) implements ReplicatorMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** 内部直接读响应。 */
    public record ReadResult(long correlationId, Optional<DataEnvelope> envelope)
            implements ReplicatorMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** 内部 Gossip 复制消息。 */
    public record Gossip(Map<String, DataEnvelope> updatedData) implements ReplicatorMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    private enum GossipTick {
        INSTANCE
    }

    private record WriteTimeoutTick(long id) {
    }

    private record ReadTimeoutTick(long id) {
    }

    private static final class PendingWrite {
        final Key<ReplicatedData> key;
        final Optional<Object> request;
        final ActorRef replyTo;
        final int requiredAcks;
        int acks = 1;
        final Cancellable timeoutTask;

        PendingWrite(
                Key<ReplicatedData> key,
                Optional<Object> request,
                ActorRef replyTo,
                int requiredAcks,
                Cancellable timeoutTask) {
            this.key = key;
            this.request = request;
            this.replyTo = replyTo;
            this.requiredAcks = requiredAcks;
            this.timeoutTask = timeoutTask;
        }
    }

    private static final class PendingRead {
        final Key<ReplicatedData> key;
        final Optional<Object> request;
        final ActorRef replyTo;
        final int requiredResponses;
        int responses = 1;
        ReplicatedData merged;
        final Cancellable timeoutTask;

        PendingRead(
                Key<ReplicatedData> key,
                Optional<Object> request,
                ActorRef replyTo,
                int requiredResponses,
                ReplicatedData initial,
                Cancellable timeoutTask) {
            this.key = key;
            this.request = request;
            this.replyTo = replyTo;
            this.requiredResponses = requiredResponses;
            this.merged = initial;
            this.timeoutTask = timeoutTask;
        }
    }

    private final ReplicatorSettings settings;
    private final Cluster cluster;
    private final UniqueAddress selfUniqueAddress;
    private final Map<String, Key<?>> keys = new HashMap<>();
    private final Map<String, DataEnvelope> dataEntries = new HashMap<>();
    private final Set<String> deletedKeys = new HashSet<>();
    private final Map<String, Set<ActorRef>> subscribers = new HashMap<>();
    private final Set<Address> nodes = new HashSet<>();
    private final Map<Long, PendingWrite> pendingWrites = new HashMap<>();
    private final Map<Long, PendingRead> pendingReads = new HashMap<>();
    private final Cancellable gossipTask;
    private long nextCorrelationId = 1L;

    /** 创建 {@link Replicator} 的 {@link Props}。 */
    public static Props props(ReplicatorSettings settings) {
        Props p = Props.create(Replicator.class, () -> new Replicator(settings));
        return settings.dispatcher().isEmpty() ? p : p.withDispatcher(settings.dispatcher());
    }

    private Replicator(ReplicatorSettings settings) {
        this.settings = settings;
        this.cluster = Cluster.get(getContext().system());
        this.selfUniqueAddress = cluster.selfUniqueAddress();
        this.gossipTask = getContext().system().scheduler().scheduleWithFixedDelay(
                settings.gossipInterval(),
                settings.gossipInterval(),
                getSelf(),
                GossipTick.INSTANCE,
                getContext().dispatcher(),
                getSelf());
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.SubscriptionInitialStateMode.InitialStateAsEvents,
                ClusterEvent.MemberEvent.class);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        gossipTask.cancel();
        cluster.unsubscribe(getSelf());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.MemberUp.class, e -> trackMember(e.member()))
                .match(ClusterEvent.MemberRemoved.class, e -> nodes.remove(e.member().address()))
                .match(ClusterEvent.MemberDowned.class, e -> nodes.remove(e.member().address()))
                .match(Update.class, u -> handleUpdate((Update<ReplicatedData>) u))
                .match(Get.class, g -> handleGet((Get<ReplicatedData>) g))
                .match(Subscribe.class, s -> handleSubscribe((Subscribe<ReplicatedData>) s))
                .match(Unsubscribe.class, u -> handleUnsubscribe(u))
                .match(Delete.class, d -> handleDelete((Delete<ReplicatedData>) d))
                .match(GetReplicaCount.class, ignored -> getSender().tell(new ReplicaCount(nodes.size()), getSelf()))
                .match(Write.class, this::handleRemoteWrite)
                .match(WriteAck.class, this::handleWriteAck)
                .match(Read.class, this::handleRemoteRead)
                .match(ReadResult.class, this::handleReadResult)
                .match(Gossip.class, this::handleGossip)
                .match(GossipTick.class, ignored -> sendGossip())
                .match(WriteTimeoutTick.class, t -> handleWriteTimeout(t.id()))
                .match(ReadTimeoutTick.class, t -> handleReadTimeout(t.id()))
                .match(Terminated.class, t -> subscribers.values().forEach(set -> set.remove(t.actor())))
                .build();
    }

    private void trackMember(Member m) {
        if (m.status() == MemberStatus.Up && !m.uniqueAddress().equals(selfUniqueAddress)) {
            if (settings.roles().isEmpty() || settings.roles().stream().allMatch(m::hasRole)) {
                nodes.add(m.address());
            }
        }
    }

    private void handleUpdate(Update<ReplicatedData> u) {
        String keyId = u.key().id();
        if (deletedKeys.contains(keyId)) {
            getSender().tell(new ModifyFailure<>(u.key(), "Key [" + keyId + "] has been deleted",
                    new IllegalStateException("Key deleted"), u.request()), getSelf());
            return;
        }
        Optional<ReplicatedData> current = Optional.ofNullable(dataEntries.get(keyId)).map(DataEnvelope::data);
        ReplicatedData updated;
        try {
            updated = u.applyModify(current);
        } catch (Throwable t) {
            getSender().tell(new ModifyFailure<>(u.key(), t.getMessage(), t, u.request()), getSelf());
            return;
        }
        keys.put(keyId, u.key());
        DataEnvelope env = new DataEnvelope(updated);
        dataEntries.put(keyId, env);
        notifySubscribers(keyId, updated);

        int required = requiredNodesForWrite(u.consistency());
        if (required <= 1 || nodes.isEmpty()) {
            getSender().tell(new UpdateSuccess<>(u.key(), u.request()), getSelf());
            return;
        }
        long cid = nextCorrelationId++;
        Cancellable timer = getContext().system().scheduler().scheduleOnce(
                u.consistency().timeout(), getSelf(), new WriteTimeoutTick(cid),
                getContext().dispatcher(), getSelf());
        pendingWrites.put(cid, new PendingWrite(u.key(), u.request(), getSender(), required, timer));
        Write writeMsg = new Write(keyId, env, cid);
        for (Address addr : nodes) {
            replicatorSelection(addr).tell(writeMsg, getSelf());
        }
    }

    private void handleGet(Get<ReplicatedData> g) {
        String keyId = g.key().id();
        ReplicatedData local = Optional.ofNullable(dataEntries.get(keyId)).map(DataEnvelope::data).orElse(null);
        int required = requiredNodesForRead(g.consistency());
        if (required <= 1 || nodes.isEmpty()) {
            if (local != null) {
                getSender().tell(new GetSuccess<>(g.key(), g.request(), local), getSelf());
            } else {
                getSender().tell(new NotFound<>(g.key(), g.request()), getSelf());
            }
            return;
        }
        long cid = nextCorrelationId++;
        Cancellable timer = getContext().system().scheduler().scheduleOnce(
                g.consistency().timeout(), getSelf(), new ReadTimeoutTick(cid),
                getContext().dispatcher(), getSelf());
        pendingReads.put(cid, new PendingRead(g.key(), g.request(), getSender(), required, local, timer));
        Read readMsg = new Read(keyId, cid);
        for (Address addr : nodes) {
            replicatorSelection(addr).tell(readMsg, getSelf());
        }
    }

    private void handleSubscribe(Subscribe<ReplicatedData> s) {
        String keyId = s.key().id();
        keys.put(keyId, s.key());
        subscribers.computeIfAbsent(keyId, ignored -> new HashSet<>()).add(s.subscriber());
        getContext().watch(s.subscriber());
        DataEnvelope env = dataEntries.get(keyId);
        if (env != null) {
            s.subscriber().tell(new Changed<>(s.key(), env.data()), getSelf());
        }
    }

    private void handleUnsubscribe(Unsubscribe<?> u) {
        Set<ActorRef> set = subscribers.get(u.key().id());
        if (set != null) {
            set.remove(u.subscriber());
        }
    }

    private void handleDelete(Delete<ReplicatedData> d) {
        String keyId = d.key().id();
        deletedKeys.add(keyId);
        dataEntries.remove(keyId);
        getSender().tell(new DeleteSuccess<>(d.key(), d.request()), getSelf());
    }

    private void handleRemoteWrite(Write w) {
        if (!deletedKeys.contains(w.keyId())) {
            mergeEntry(w.keyId(), w.envelope());
        }
        getSender().tell(new WriteAck(w.correlationId()), getSelf());
    }

    private void handleWriteAck(WriteAck ack) {
        PendingWrite pw = pendingWrites.get(ack.correlationId());
        if (pw != null) {
            pw.acks++;
            if (pw.acks >= pw.requiredAcks) {
                pendingWrites.remove(ack.correlationId());
                pw.timeoutTask.cancel();
                pw.replyTo.tell(new UpdateSuccess<>(pw.key, pw.request), getSelf());
            }
        }
    }

    private void handleWriteTimeout(long id) {
        PendingWrite pw = pendingWrites.remove(id);
        if (pw != null) {
            pw.replyTo.tell(new UpdateTimeout<>(pw.key, pw.request), getSelf());
        }
    }

    private void handleRemoteRead(Read r) {
        Optional<DataEnvelope> env = Optional.ofNullable(dataEntries.get(r.keyId()));
        getSender().tell(new ReadResult(r.correlationId(), env), getSelf());
    }

    private void handleReadResult(ReadResult res) {
        PendingRead pr = pendingReads.get(res.correlationId());
        if (pr != null) {
            pr.responses++;
            if (res.envelope().isPresent()) {
                ReplicatedData incoming = res.envelope().get().data();
                pr.merged = pr.merged == null ? incoming : pr.merged.mergeData(incoming);
                mergeEntry(pr.key.id(), new DataEnvelope(pr.merged));
            }
            if (pr.responses >= pr.requiredResponses) {
                pendingReads.remove(res.correlationId());
                pr.timeoutTask.cancel();
                if (pr.merged != null) {
                    pr.replyTo.tell(new GetSuccess<>(pr.key, pr.request, pr.merged), getSelf());
                } else {
                    pr.replyTo.tell(new NotFound<>(pr.key, pr.request), getSelf());
                }
            }
        }
    }

    private void handleReadTimeout(long id) {
        PendingRead pr = pendingReads.remove(id);
        if (pr != null) {
            pr.replyTo.tell(new GetFailure<>(pr.key, pr.request), getSelf());
        }
    }

    private void handleGossip(Gossip g) {
        for (Map.Entry<String, DataEnvelope> entry : g.updatedData().entrySet()) {
            if (!deletedKeys.contains(entry.getKey())) {
                mergeEntry(entry.getKey(), entry.getValue());
            }
        }
    }

    private void sendGossip() {
        if (nodes.isEmpty() || dataEntries.isEmpty()) {
            return;
        }
        List<Address> list = new ArrayList<>(nodes);
        Address target = list.get(ThreadLocalRandom.current().nextInt(list.size()));
        replicatorSelection(target).tell(new Gossip(Map.copyOf(dataEntries)), getSelf());
    }

    private void mergeEntry(String keyId, DataEnvelope incoming) {
        DataEnvelope existing = dataEntries.get(keyId);
        ReplicatedData merged = existing == null
                ? incoming.data()
                : existing.data().mergeData(incoming.data());
        if (existing == null || !existing.data().equals(merged)) {
            dataEntries.put(keyId, new DataEnvelope(merged));
            notifySubscribers(keyId, merged);
        }
    }

    @SuppressWarnings("unchecked")
    private void notifySubscribers(String keyId, ReplicatedData data) {
        Set<ActorRef> subs = subscribers.get(keyId);
        Key<ReplicatedData> key = (Key<ReplicatedData>) keys.get(keyId);
        if (subs != null && key != null && !subs.isEmpty()) {
            Changed<ReplicatedData> msg = new Changed<>(key, data);
            for (ActorRef sub : new ArrayList<>(subs)) {
                sub.tell(msg, getSelf());
            }
        }
    }

    private int requiredNodesForWrite(WriteConsistency c) {
        int total = nodes.size() + 1;
        return switch (c) {
            case WriteLocal ignored -> 1;
            case WriteTo wt -> wt.n();
            case WriteMajority wm -> Math.min(total, Math.max(wm.minCap(), (total / 2) + 1));
            case WriteAll ignored -> total;
        };
    }

    private int requiredNodesForRead(ReadConsistency c) {
        int total = nodes.size() + 1;
        return switch (c) {
            case ReadLocal ignored -> 1;
            case ReadFrom rf -> rf.n();
            case ReadMajority rm -> Math.min(total, Math.max(rm.minCap(), (total / 2) + 1));
            case ReadAll ignored -> total;
        };
    }

    private ActorSelection replicatorSelection(Address address) {
        return getContext().actorSelection(address + getSelf().path().toStringWithoutAddress());
    }
}
