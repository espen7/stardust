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
package io.github.espen7.tapio.cluster;

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.CoordinatedShutdown;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;
import io.github.espen7.tapio.remote.FailureDetectorRegistry;
import io.github.espen7.tapio.remote.RemoteActorRefProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * INTERNAL API：集群成员管理核心守护 Actor，驱动 Gossip 传播、成员状态机、Leader 动作、可达性探测与订阅分发。
 */
@InternalApi
public final class ClusterDaemon extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    /** 订阅集群事件内部命令。 */
    public record Subscribe(ActorRef subscriber, ClusterEvent.SubscriptionInitialStateMode initialMode,
                            Set<Class<?>> to) {
    }

    /** 退订集群事件内部命令。 */
    public record Unsubscribe(ActorRef subscriber, Optional<Class<?>> to) {
    }

    /** 发送当前状态快照命令。 */
    public record SendCurrentClusterState(ActorRef receiver) {
    }

    /** 注册 MemberUp 回调命令。 */
    public record RegisterOnMemberUp(Runnable callback) {
    }

    private enum GossipTick {
        INSTANCE
    }

    private enum LeaderActionsTick {
        INSTANCE
    }

    private enum ReapUnreachableTick {
        INSTANCE
    }

    private enum JoinSeedTick {
        INSTANCE
    }

    private final ClusterSettings settings;
    private final UniqueAddress selfUniqueAddress;
    private final String selfVClockNode;
    private final ClusterReadView readView;
    private final FailureDetectorRegistry<Address> failureDetector;
    private final ActorRef heartbeatSender;
    private final Map<ActorRef, Set<Class<?>>> subscribers = new HashMap<>();
    private final List<Runnable> onMemberUpCallbacks = new ArrayList<>();
    private final List<Runnable> onMemberRemovedCallbacks = new ArrayList<>();
    private final Map<UniqueAddress, Instant> downDetectedAt = new HashMap<>();

    private Gossip latestGossip = Gossip.EMPTY;
    private ClusterEvent.CurrentClusterState publishedState = ClusterEvent.CurrentClusterState.EMPTY;
    private boolean joined = false;
    private boolean joiningStarted = false;
    private boolean selfRemoved = false;
    private List<Address> pendingSeedNodes = List.of();
    private Instant joinFirstAttemptAt;

    private final Cancellable gossipTask;
    private final Cancellable leaderTask;
    private final Cancellable reaperTask;
    private Cancellable seedJoinTask;

    /**
     * 构造集群核心守护 Actor。
     *
     * @param settings 集群配置
     * @param selfUniqueAddress 本节点唯一地址
     * @param readView 只读状态视图
     * @param failureDetector 失效检测器注册表
     */
    public ClusterDaemon(
            ClusterSettings settings,
            UniqueAddress selfUniqueAddress,
            ClusterReadView readView,
            FailureDetectorRegistry<Address> failureDetector) {
        this.settings = settings;
        this.selfUniqueAddress = selfUniqueAddress;
        this.selfVClockNode = Gossip.vclockName(selfUniqueAddress);
        this.readView = readView;
        this.failureDetector = failureDetector;
        this.heartbeatSender = getContext().actorOf(
                ClusterHeartbeatSender.props(settings, selfUniqueAddress, failureDetector), "heartbeatSender");

        this.gossipTask = getContext().system().scheduler().scheduleWithFixedDelay(
                settings.GossipInterval, settings.GossipInterval, getSelf(), GossipTick.INSTANCE,
                getContext().dispatcher(), getSelf());
        this.leaderTask = getContext().system().scheduler().scheduleWithFixedDelay(
                settings.LeaderActionsInterval, settings.LeaderActionsInterval, getSelf(), LeaderActionsTick.INSTANCE,
                getContext().dispatcher(), getSelf());
        this.reaperTask = getContext().system().scheduler().scheduleWithFixedDelay(
                settings.UnreachableNodesReaperInterval, settings.UnreachableNodesReaperInterval,
                getSelf(), ReapUnreachableTick.INSTANCE, getContext().dispatcher(), getSelf());

        startDowningProvider();
    }

    /** 创建 {@link Props}。 */
    public static Props props(
            ClusterSettings settings,
            UniqueAddress selfUniqueAddress,
            ClusterReadView readView,
            FailureDetectorRegistry<Address> failureDetector) {
        return Props.create(ClusterDaemon.class,
                () -> new ClusterDaemon(settings, selfUniqueAddress, readView, failureDetector));
    }

    private void startDowningProvider() {
        try {
            ExtendedActorSystem sys = (ExtendedActorSystem) getContext().system();
            DowningProvider provider = sys.dynamicAccess().createInstanceFor(
                    settings.DowningProviderClassName,
                    DowningProvider.class,
                    List.of(Map.entry(io.github.espen7.tapio.actor.ActorSystem.class, sys)));
            provider.downingActorProps().ifPresent(p -> getContext().actorOf(p, "downingProvider"));
        } catch (Throwable t) {
            log().warning("Failed to start DowningProvider [{}]: {}", settings.DowningProviderClassName, t.getMessage());
        }
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        gossipTask.cancel();
        leaderTask.cancel();
        reaperTask.cancel();
        if (seedJoinTask != null) {
            seedJoinTask.cancel();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterMessages.JoinSeedNodes.class, cmd -> joinSeedNodes(cmd.seedNodes()))
                .match(ClusterMessages.JoinTo.class, cmd -> joinAddress(cmd.address()))
                .match(JoinSeedTick.class, ignored -> retryJoinSeeds())
                .match(ClusterMessages.InitJoin.class, ignored -> handleInitJoin())
                .match(ClusterMessages.InitJoinAck.class, ack -> handleInitJoinAck(ack.address()))
                .match(ClusterMessages.Join.class, this::handleJoin)
                .match(ClusterMessages.Welcome.class, this::handleWelcome)
                .match(GossipTick.class, ignored -> gossip())
                .match(ClusterMessages.GossipStatus.class, this::handleGossipStatus)
                .match(ClusterMessages.GossipEnvelope.class, this::handleGossipEnvelope)
                .match(LeaderActionsTick.class, ignored -> leaderActions())
                .match(ReapUnreachableTick.class, ignored -> reapUnreachable())
                .match(ClusterMessages.Leave.class, cmd -> leave(cmd.address()))
                .match(ClusterMessages.Down.class, cmd -> down(cmd.address()))
                .match(ClusterMessages.ExitingConfirmed.class, cmd -> handleExitingConfirmed(cmd.node()))
                .match(Subscribe.class, this::handleSubscribe)
                .match(Unsubscribe.class, this::handleUnsubscribe)
                .match(SendCurrentClusterState.class, cmd -> cmd.receiver().tell(publishedState, getSelf()))
                .match(RegisterOnMemberUp.class, cmd -> registerOnMemberUp(cmd.callback()))
                .match(Runnable.class, Runnable::run)
                .build();
    }

    /** 注册 MemberUp 回调。 */
    void registerOnMemberUp(Runnable callback) {
        if (readView.selfMember().status() == MemberStatus.Up) {
            callback.run();
        } else {
            onMemberUpCallbacks.add(callback);
        }
    }

    /** 注册 MemberRemoved 回调。 */
    void registerOnMemberRemoved(Runnable callback) {
        if (selfRemoved) {
            callback.run();
        } else {
            onMemberRemovedCallbacks.add(callback);
        }
    }

    private void joinSeedNodes(List<Address> seedNodes) {
        if (joined || seedNodes.isEmpty()) {
            return;
        }
        this.pendingSeedNodes = List.copyOf(seedNodes);
        this.joiningStarted = true;
        if (seedNodes.getFirst().equals(selfUniqueAddress.address())) {
            joinSelf();
            return;
        }
        this.joinFirstAttemptAt = Instant.now();
        sendInitJoins();
        if (seedJoinTask == null) {
            seedJoinTask = getContext().system().scheduler().scheduleWithFixedDelay(
                    settings.RetryUnsuccessfulJoinAfter,
                    settings.RetryUnsuccessfulJoinAfter,
                    getSelf(),
                    JoinSeedTick.INSTANCE,
                    getContext().dispatcher(),
                    getSelf());
        }
    }

    private void retryJoinSeeds() {
        if (joined || pendingSeedNodes.isEmpty()) {
            if (seedJoinTask != null) {
                seedJoinTask.cancel();
                seedJoinTask = null;
            }
            return;
        }
        if (settings.ShutdownAfterUnsuccessfulJoinSeedNodes.isPresent()
                && joinFirstAttemptAt != null
                && Instant.now().isAfter(joinFirstAttemptAt.plus(settings.ShutdownAfterUnsuccessfulJoinSeedNodes.get()))) {
            log().warning("Shutting down after unsuccessful join to seed nodes");
            CoordinatedShutdown.get(getContext().system()).run(
                    CoordinatedShutdown.ClusterDowningReason.INSTANCE, Optional.empty());
            return;
        }
        sendInitJoins();
    }

    private void sendInitJoins() {
        for (Address seed : pendingSeedNodes) {
            if (!seed.equals(selfUniqueAddress.address())) {
                daemonSelection(seed).tell(ClusterMessages.InitJoin.INSTANCE, getSelf());
            }
        }
    }

    private void joinAddress(Address address) {
        if (joined) {
            return;
        }
        if (address.equals(selfUniqueAddress.address())) {
            joinSelf();
        } else {
            daemonSelection(address).tell(
                    new ClusterMessages.Join(selfUniqueAddress, settings.Roles, settings.AppVersion), getSelf());
        }
    }

    private void joinSelf() {
        if (joined) {
            return;
        }
        joined = true;
        if (seedJoinTask != null) {
            seedJoinTask.cancel();
            seedJoinTask = null;
        }
        Member selfMember = Member.create(selfUniqueAddress, settings.Roles, settings.AppVersion);
        NavigableSet<Member> members = new TreeSet<>(Member.ORDERING);
        members.add(selfMember);
        Gossip g = new Gossip(members, new GossipOverview(Set.of(selfUniqueAddress), Reachability.EMPTY),
                VectorClock.EMPTY.increment(selfVClockNode), Map.of());
        updateLatestGossip(g);
        leaderActions();
    }

    private void handleInitJoin() {
        if (joined) {
            getSender().tell(new ClusterMessages.InitJoinAck(selfUniqueAddress.address()), getSelf());
        } else {
            getSender().tell(new ClusterMessages.InitJoinNack(selfUniqueAddress.address()), getSelf());
        }
    }

    private void handleInitJoinAck(Address seedAddr) {
        if (!joined) {
            daemonSelection(seedAddr).tell(
                    new ClusterMessages.Join(selfUniqueAddress, settings.Roles, settings.AppVersion), getSelf());
        }
    }

    private void handleJoin(ClusterMessages.Join join) {
        if (!joined) {
            return;
        }
        UniqueAddress joiningNode = join.node();
        if (latestGossip.tombstones().containsKey(joiningNode)) {
            return;
        }
        if (!latestGossip.hasMember(joiningNode)) {
            NavigableSet<Member> nextMembers = new TreeSet<>(Member.ORDERING);
            nextMembers.addAll(latestGossip.members());
            nextMembers.add(Member.create(joiningNode, join.roles(), join.appVersion()));
            Gossip next = latestGossip.withMembers(nextMembers).increment(selfVClockNode, selfUniqueAddress);
            updateLatestGossip(next);
            log().info("Cluster Node [{}] - Node [{}] is JOINING, roles [{}]",
                    selfUniqueAddress.address(), joiningNode.address(), join.roles());
        }
        getSender().tell(new ClusterMessages.Welcome(selfUniqueAddress, latestGossip), getSelf());
    }

    private void handleWelcome(ClusterMessages.Welcome welcome) {
        if (joined) {
            return;
        }
        joined = true;
        if (seedJoinTask != null) {
            seedJoinTask.cancel();
            seedJoinTask = null;
        }
        Gossip merged = latestGossip.merge(welcome.gossip()).seen(selfUniqueAddress);
        updateLatestGossip(merged);
        log().info("Cluster Node [{}] - Welcome from [{}]", selfUniqueAddress.address(), welcome.from().address());
    }

    private void gossip() {
        if (!joined || latestGossip.members().size() <= 1) {
            return;
        }
        List<Member> peers = new ArrayList<>();
        Set<UniqueAddress> unreachable = latestGossip.overview().reachability().allUnreachableOrTerminated();
        for (Member m : latestGossip.members()) {
            if (!m.uniqueAddress().equals(selfUniqueAddress)
                    && m.status() != MemberStatus.Down
                    && !unreachable.contains(m.uniqueAddress())) {
                peers.add(m);
            }
        }
        if (peers.isEmpty()) {
            return;
        }
        Member target = peers.get(ThreadLocalRandom.current().nextInt(peers.size()));
        if (latestGossip.overview().seen().contains(target.uniqueAddress())) {
            daemonSelection(target.address()).tell(
                    new ClusterMessages.GossipStatus(
                            selfUniqueAddress, latestGossip.version(), latestGossip.overview().seen()),
                    getSelf());
        } else {
            daemonSelection(target.address()).tell(
                    new ClusterMessages.GossipEnvelope(selfUniqueAddress, target.uniqueAddress(), latestGossip),
                    getSelf());
        }
    }

    private void handleGossipStatus(ClusterMessages.GossipStatus status) {
        if (!joined || !latestGossip.hasMember(status.from())) {
            return;
        }
        VectorClock.Ordering cmp = status.version().compareTo(latestGossip.version());
        if (cmp == VectorClock.Ordering.Before || cmp == VectorClock.Ordering.Concurrent) {
            getSender().tell(
                    new ClusterMessages.GossipEnvelope(selfUniqueAddress, status.from(), latestGossip), getSelf());
        } else if (cmp == VectorClock.Ordering.After) {
            getSender().tell(
                    new ClusterMessages.GossipStatus(
                            selfUniqueAddress, latestGossip.version(), latestGossip.overview().seen()),
                    getSelf());
        } else if (cmp == VectorClock.Ordering.Same) {
            if (!status.seen().containsAll(latestGossip.overview().seen())) {
                getSender().tell(
                        new ClusterMessages.GossipEnvelope(selfUniqueAddress, status.from(), latestGossip), getSelf());
            } else if (!latestGossip.overview().seen().containsAll(status.seen())) {
                getSender().tell(
                        new ClusterMessages.GossipStatus(
                                selfUniqueAddress, latestGossip.version(), latestGossip.overview().seen()),
                        getSelf());
            }
        }
    }

    private void handleGossipEnvelope(ClusterMessages.GossipEnvelope env) {
        if (!joined || !env.to().equals(selfUniqueAddress)) {
            return;
        }
        Gossip merged = latestGossip.merge(env.gossip()).seen(selfUniqueAddress);
        updateLatestGossip(merged);
    }

    private void leaderActions() {
        if (!joined) {
            return;
        }
        if (!latestGossip.isLeader(selfUniqueAddress, settings.SelfDataCenter)) {
            return;
        }
        boolean convergence = latestGossip.convergence(selfUniqueAddress);
        NavigableSet<Member> nextMembers = new TreeSet<>(Member.ORDERING);
        Map<UniqueAddress, Long> nextTombstones = new HashMap<>(latestGossip.tombstones());
        Set<UniqueAddress> removedNodes = new HashSet<>();
        boolean changed = false;

        int maxUpNumber = latestGossip.members().stream()
                .mapToInt(m -> m.upNumber() == Integer.MAX_VALUE ? 0 : m.upNumber())
                .max()
                .orElse(0);

        boolean minMembersMet = latestGossip.members().size() >= settings.MinNrOfMembers;
        for (Map.Entry<String, Integer> req : settings.MinNrOfMembersOfRole.entrySet()) {
            long count = latestGossip.members().stream().filter(m -> m.hasRole(req.getKey())).count();
            if (count < req.getValue()) {
                minMembersMet = false;
                break;
            }
        }

        for (Member m : latestGossip.members()) {
            if (m.status() == MemberStatus.Down) {
                Instant detected = downDetectedAt.computeIfAbsent(m.uniqueAddress(), ignored -> Instant.now());
                if (!Instant.now().isBefore(detected.plus(settings.DownRemovalMargin))) {
                    removedNodes.add(m.uniqueAddress());
                    nextTombstones.put(m.uniqueAddress(), System.currentTimeMillis());
                    changed = true;
                    continue;
                }
            } else if (m.status() == MemberStatus.Exiting && convergence) {
                daemonSelection(m.address()).tell(new ClusterMessages.ExitingConfirmed(m.uniqueAddress()), getSelf());
                removedNodes.add(m.uniqueAddress());
                nextTombstones.put(m.uniqueAddress(), System.currentTimeMillis());
                changed = true;
                continue;
            } else if (m.status() == MemberStatus.Leaving && convergence) {
                nextMembers.add(m.copy(MemberStatus.Exiting));
                changed = true;
                continue;
            } else if ((m.status() == MemberStatus.Joining || m.status() == MemberStatus.WeaklyUp)
                    && convergence && minMembersMet) {
                nextMembers.add(m.copyUp(++maxUpNumber));
                changed = true;
                continue;
            }
            nextMembers.add(m);
        }

        if (changed) {
            Gossip next = latestGossip.withMembers(nextMembers).withTombstones(nextTombstones);
            if (!removedNodes.isEmpty()) {
                next = next.withReachability(next.overview().reachability().remove(removedNodes));
                for (UniqueAddress rem : removedNodes) {
                    if (!rem.equals(selfUniqueAddress)
                            && getContext().system() instanceof ExtendedActorSystem ext
                            && ext.provider() instanceof RemoteActorRefProvider rarp) {
                        rarp.quarantine(rem.address(), Optional.of(rem.longUid()), "Removed from cluster");
                    }
                }
            }
            next = next.increment(selfVClockNode, selfUniqueAddress);
            updateLatestGossip(next);
        }
    }

    private void reapUnreachable() {
        if (!joined) {
            return;
        }
        Reachability reach = latestGossip.overview().reachability();
        Reachability nextReach = reach;
        for (Member m : latestGossip.members()) {
            if (m.uniqueAddress().equals(selfUniqueAddress) || m.status() == MemberStatus.Down) {
                continue;
            }
            boolean available = failureDetector.isAvailable(m.address());
            boolean currentlyMarked = !reach.isReachable(m.uniqueAddress());
            if (!available && !currentlyMarked) {
                nextReach = nextReach.unreachable(selfUniqueAddress, m.uniqueAddress());
            } else if (available && currentlyMarked) {
                nextReach = nextReach.reachable(selfUniqueAddress, m.uniqueAddress());
            }
        }
        if (!nextReach.equals(reach)) {
            updateLatestGossip(latestGossip.withReachability(nextReach).seen(selfUniqueAddress));
        }
    }

    private void leave(Address address) {
        if (!joined) {
            return;
        }
        NavigableSet<Member> nextMembers = new TreeSet<>(Member.ORDERING);
        boolean changed = false;
        for (Member m : latestGossip.members()) {
            if (m.address().equals(address)
                    && (m.status() == MemberStatus.Joining
                    || m.status() == MemberStatus.WeaklyUp
                    || m.status() == MemberStatus.Up)) {
                nextMembers.add(m.copy(MemberStatus.Leaving));
                changed = true;
            } else {
                nextMembers.add(m);
            }
        }
        if (changed) {
            updateLatestGossip(latestGossip.withMembers(nextMembers).increment(selfVClockNode, selfUniqueAddress));
        }
    }

    private void down(Address address) {
        if (!joined) {
            return;
        }
        NavigableSet<Member> nextMembers = new TreeSet<>(Member.ORDERING);
        boolean changed = false;
        for (Member m : latestGossip.members()) {
            if (m.address().equals(address) && m.status() != MemberStatus.Down && m.status() != MemberStatus.Removed) {
                nextMembers.add(m.copy(MemberStatus.Down));
                downDetectedAt.putIfAbsent(m.uniqueAddress(), Instant.now());
                changed = true;
            } else {
                nextMembers.add(m);
            }
        }
        if (changed) {
            updateLatestGossip(latestGossip.withMembers(nextMembers).increment(selfVClockNode, selfUniqueAddress));
        }
    }

    private void handleExitingConfirmed(UniqueAddress node) {
        if (node.equals(selfUniqueAddress)) {
            shutdownSelf(CoordinatedShutdown.ClusterLeavingReason.INSTANCE);
        }
    }

    private void shutdownSelf(CoordinatedShutdown.Reason reason) {
        if (!selfRemoved) {
            selfRemoved = true;
            publish(ClusterEvent.ClusterShuttingDown.INSTANCE);
            for (Runnable cb : onMemberRemovedCallbacks) {
                try {
                    cb.run();
                } catch (Throwable ignored) {
                }
            }
            onMemberRemovedCallbacks.clear();
            CoordinatedShutdown.get(getContext().system()).run(reason, Optional.empty());
        }
    }

    private void updateLatestGossip(Gossip next) {
        Gossip prev = this.latestGossip;
        this.latestGossip = next;
        readView.refresh(next, selfUniqueAddress, settings.SelfDataCenter);
        ClusterEvent.CurrentClusterState prevState = this.publishedState;
        ClusterEvent.CurrentClusterState nextState = readView.state();
        this.publishedState = nextState;

        Set<UniqueAddress> memberUas = new HashSet<>();
        for (Member m : next.members()) {
            memberUas.add(m.uniqueAddress());
        }
        heartbeatSender.tell(new ClusterHeartbeatSender.UpdateHeartbeatTargets(memberUas), getSelf());

        publishDiffEvents(prev, next, prevState, nextState);

        if (readView.selfMember().status() == MemberStatus.Up && !onMemberUpCallbacks.isEmpty()) {
            List<Runnable> cbs = new ArrayList<>(onMemberUpCallbacks);
            onMemberUpCallbacks.clear();
            cbs.forEach(Runnable::run);
        }
        if (next.hasMember(selfUniqueAddress)
                && next.member(selfUniqueAddress).status() == MemberStatus.Down
                && settings.RunCoordinatedShutdownWhenDown) {
            shutdownSelf(CoordinatedShutdown.ClusterDowningReason.INSTANCE);
        }
    }

    private void publishDiffEvents(
            Gossip prev,
            Gossip next,
            ClusterEvent.CurrentClusterState prevState,
            ClusterEvent.CurrentClusterState nextState) {
        for (Member m : next.members()) {
            if (!prev.hasMember(m.uniqueAddress())) {
                publishMemberEvent(m);
            } else {
                Member old = prev.member(m.uniqueAddress());
                if (old.status() != m.status()) {
                    publishMemberEvent(m);
                }
            }
        }
        for (Member old : prev.members()) {
            if (!next.hasMember(old.uniqueAddress())) {
                Member removed = old.status() == MemberStatus.Removed ? old : Member.removed(old.uniqueAddress());
                publish(new ClusterEvent.MemberRemoved(removed, old.status()));
            }
        }

        for (Member m : nextState.unreachable()) {
            if (!prevState.unreachable().contains(m)) {
                publish(new ClusterEvent.UnreachableMember(m));
            }
        }
        for (Member m : prevState.unreachable()) {
            if (!nextState.unreachable().contains(m) && next.hasMember(m.uniqueAddress())) {
                publish(new ClusterEvent.ReachableMember(next.member(m.uniqueAddress())));
            }
        }

        if (!prevState.leader().equals(nextState.leader())) {
            publish(new ClusterEvent.LeaderChanged(nextState.leader()));
        }
        for (Map.Entry<String, Optional<Address>> e : nextState.roleLeaderMap().entrySet()) {
            Optional<Address> oldLeader = prevState.roleLeaderMap().getOrDefault(e.getKey(), Optional.empty());
            if (!oldLeader.equals(e.getValue())) {
                publish(new ClusterEvent.RoleLeaderChanged(e.getKey(), e.getValue()));
            }
        }
    }

    private void publishMemberEvent(Member m) {
        switch (m.status()) {
            case Joining -> publish(new ClusterEvent.MemberJoined(m));
            case WeaklyUp -> publish(new ClusterEvent.MemberWeaklyUp(m));
            case Up -> publish(new ClusterEvent.MemberUp(m));
            case Leaving -> publish(new ClusterEvent.MemberLeft(m));
            case Exiting -> publish(new ClusterEvent.MemberExited(m));
            case Down -> publish(new ClusterEvent.MemberDowned(m));
            case PreparingForShutdown -> publish(new ClusterEvent.MemberPreparingForShutdown(m));
            case ReadyForShutdown -> publish(new ClusterEvent.MemberReadyForShutdown(m));
            case Removed -> { }
        }
    }

    private void publish(ClusterEvent.ClusterDomainEvent event) {
        for (Map.Entry<ActorRef, Set<Class<?>>> entry : new ArrayList<>(subscribers.entrySet())) {
            for (Class<?> c : entry.getValue()) {
                if (c.isInstance(event)) {
                    entry.getKey().tell(event, getSelf());
                    break;
                }
            }
        }
    }

    private void handleSubscribe(Subscribe sub) {
        subscribers.computeIfAbsent(sub.subscriber(), ignored -> new HashSet<>()).addAll(sub.to());
        if (sub.initialMode() == ClusterEvent.SubscriptionInitialStateMode.InitialStateAsSnapshot) {
            sub.subscriber().tell(publishedState, getSelf());
        } else {
            for (Member m : publishedState.members()) {
                publishInitialMemberEvent(sub.subscriber(), sub.to(), m);
            }
            for (Member u : publishedState.unreachable()) {
                deliverIfSubscribed(sub.subscriber(), sub.to(), new ClusterEvent.UnreachableMember(u));
            }
            if (publishedState.leader().isPresent()) {
                deliverIfSubscribed(sub.subscriber(), sub.to(),
                        new ClusterEvent.LeaderChanged(publishedState.leader()));
            }
        }
    }

    private void publishInitialMemberEvent(ActorRef sub, Set<Class<?>> to, Member m) {
        switch (m.status()) {
            case Joining -> deliverIfSubscribed(sub, to, new ClusterEvent.MemberJoined(m));
            case WeaklyUp -> deliverIfSubscribed(sub, to, new ClusterEvent.MemberWeaklyUp(m));
            case Up -> deliverIfSubscribed(sub, to, new ClusterEvent.MemberUp(m));
            case Leaving -> {
                deliverIfSubscribed(sub, to, new ClusterEvent.MemberUp(m));
                deliverIfSubscribed(sub, to, new ClusterEvent.MemberLeft(m));
            }
            case Exiting -> deliverIfSubscribed(sub, to, new ClusterEvent.MemberExited(m));
            case Down -> deliverIfSubscribed(sub, to, new ClusterEvent.MemberDowned(m));
            default -> { }
        }
    }

    private void deliverIfSubscribed(ActorRef sub, Set<Class<?>> to, ClusterEvent.ClusterDomainEvent ev) {
        for (Class<?> c : to) {
            if (c.isInstance(ev)) {
                sub.tell(ev, getSelf());
                return;
            }
        }
    }

    private void handleUnsubscribe(Unsubscribe unsub) {
        if (unsub.to().isEmpty()) {
            subscribers.remove(unsub.subscriber());
        } else {
            Set<Class<?>> set = subscribers.get(unsub.subscriber());
            if (set != null) {
                set.remove(unsub.to().get());
                if (set.isEmpty()) {
                    subscribers.remove(unsub.subscriber());
                }
            }
        }
    }

    private ActorSelection daemonSelection(Address address) {
        return getContext().actorSelection(address + "/system/cluster-core-daemon");
    }
}
