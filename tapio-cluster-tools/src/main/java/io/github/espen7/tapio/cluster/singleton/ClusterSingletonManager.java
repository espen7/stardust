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
package io.github.espen7.tapio.cluster.singleton;

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.coordination.lease.Lease;
import io.github.espen7.tapio.coordination.lease.LeaseProvider;
import io.github.espen7.tapio.coordination.lease.LeaseUsageSettings;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;

import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * 在集群中最老的匹配节点上托管唯一单例 Actor，并通过 HandOver 握手协议在主节点迁移时确保旧单例先停止再启动新单例。
 */
public final class ClusterSingletonManager extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    /** INTERNAL API：新最老节点向旧最老节点发起接管请求。 */
    @InternalApi
    public enum HandOverToMe implements ClusterSingletonMessage, DeadLetterSuppression {
        INSTANCE
    }

    /** INTERNAL API：旧最老节点确认正在停止旧单例。 */
    @InternalApi
    public enum HandOverInProgress implements ClusterSingletonMessage {
        INSTANCE
    }

    /** INTERNAL API：旧最老节点通知旧单例已彻底终止，可以启动新单例。 */
    @InternalApi
    public enum HandOverDone implements ClusterSingletonMessage {
        INSTANCE
    }

    /** INTERNAL API：旧最老节点在退位时主动通知新最老节点接管。 */
    @InternalApi
    public enum TakeOverFromMe implements ClusterSingletonMessage, DeadLetterSuppression {
        INSTANCE
    }

    private enum State {
        START,
        YOUNGER,
        BECOMING_OLDEST,
        OLDEST,
        WAS_OLDEST,
        HANDING_OVER,
        END
    }

    private enum HandOverRetryTick {
        INSTANCE
    }

    private enum TakeOverRetryTick {
        INSTANCE
    }

    private record AcquireLeaseResult(boolean acquired) {
    }

    private enum RetryAcquireLeaseTick {
        INSTANCE
    }

    private record LeaseLost(Optional<Throwable> reason) {
    }

    private static final Comparator<Member> AGE_ORDERING = (a, b) -> {
        if (a.equals(b)) return 0;
        return a.isOlderThan(b) ? -1 : 1;
    };

    private final Props singletonProps;
    private final Object terminationMessage;
    private final ClusterSingletonManagerSettings settings;
    private final Cluster cluster;
    private final NavigableSet<Member> oldestChangedBuffer = new TreeSet<>(AGE_ORDERING);
    private final Lease lease;
    private final LeaseUsageSettings leaseUsageSettings;
    private final Set<ActorRef> handOverTo = new HashSet<>();

    private State state = State.START;
    private ActorRef singleton;
    private boolean selfUp = false;
    private boolean selfExited = false;
    private Member previousOldest;
    private Member newOldest;
    private Cancellable retryTask;
    private boolean acquiringLease = false;
    private boolean holdingLease = false;

    /**
     * 创建 {@link ClusterSingletonManager} 的 {@link Props}。
     */
    public static Props props(
            Props singletonProps,
            Object terminationMessage,
            ClusterSingletonManagerSettings settings) {
        return Props.create(ClusterSingletonManager.class,
                () -> new ClusterSingletonManager(singletonProps, terminationMessage, settings));
    }

    private ClusterSingletonManager(
            Props singletonProps,
            Object terminationMessage,
            ClusterSingletonManagerSettings settings) {
        this.singletonProps = singletonProps;
        this.terminationMessage = terminationMessage;
        this.settings = settings;
        this.cluster = Cluster.get(getContext().system());
        if (settings.leaseUsageSettings().isPresent()) {
            this.leaseUsageSettings = settings.leaseUsageSettings().get();
            String leaseName = leaseUsageSettings.getLeaseName().isEmpty()
                    ? getContext().system().settings().name() + "-singleton-" + getSelf().path().toStringWithoutAddress()
                    : leaseUsageSettings.getLeaseName();
            this.lease = LeaseProvider.get(getContext().system())
                    .getLease(leaseName, leaseUsageSettings.leaseImplementation(), cluster.selfAddress().toString());
        } else {
            this.leaseUsageSettings = null;
            this.lease = null;
        }
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.SubscriptionInitialStateMode.InitialStateAsEvents,
                ClusterEvent.MemberEvent.class);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        cancelRetry();
        cluster.unsubscribe(getSelf());
        if (lease != null && holdingLease) {
            lease.release();
            holdingLease = false;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.MemberUp.class, e -> addMember(e.member()))
                .match(ClusterEvent.MemberLeft.class, e -> removeOrUpdateMember(e.member()))
                .match(ClusterEvent.MemberExited.class, e -> {
                    if (e.member().uniqueAddress().equals(cluster.selfUniqueAddress())) {
                        selfExited = true;
                    }
                    removeOrUpdateMember(e.member());
                })
                .match(ClusterEvent.MemberDowned.class, e -> removeOrUpdateMember(e.member()))
                .match(ClusterEvent.MemberRemoved.class, e -> onMemberRemoved(e.member()))
                .match(HandOverToMe.class, ignored -> onHandOverToMe(getSender()))
                .match(HandOverInProgress.class, ignored -> cancelRetry())
                .match(HandOverDone.class, ignored -> onHandOverDone(getSender()))
                .match(TakeOverFromMe.class, ignored -> onTakeOverFromMe(getSender()))
                .match(HandOverRetryTick.class, ignored -> sendHandOverToMe())
                .match(TakeOverRetryTick.class, ignored -> sendTakeOverFromMe())
                .match(AcquireLeaseResult.class, this::onAcquireLeaseResult)
                .match(RetryAcquireLeaseTick.class, ignored -> tryStartOldest())
                .match(LeaseLost.class, this::onLeaseLost)
                .match(Terminated.class, this::onTerminated)
                .build();
    }

    private boolean matchesRole(Member m) {
        return settings.role().map(m::hasRole).orElse(true);
    }

    private void addMember(Member m) {
        if (!matchesRole(m)) {
            return;
        }
        if (m.uniqueAddress().equals(cluster.selfUniqueAddress())) {
            selfUp = true;
            if (state == State.START) {
                state = State.YOUNGER;
            }
        }
        oldestChangedBuffer.remove(m);
        oldestChangedBuffer.add(m);
        evaluateOldest();
    }

    private void removeOrUpdateMember(Member m) {
        if (!matchesRole(m)) {
            return;
        }
        oldestChangedBuffer.remove(m);
        if (m.status() == MemberStatus.Up) {
            oldestChangedBuffer.add(m);
        }
        evaluateOldest();
    }

    private void onMemberRemoved(Member m) {
        if (!matchesRole(m)) {
            return;
        }
        oldestChangedBuffer.remove(m);
        if (state == State.BECOMING_OLDEST && previousOldest != null
                && previousOldest.uniqueAddress().equals(m.uniqueAddress())) {
            cancelRetry();
            previousOldest = null;
            tryStartOldest();
            return;
        }
        evaluateOldest();
    }

    private void evaluateOldest() {
        if (!selfUp || oldestChangedBuffer.isEmpty() || state == State.END) {
            return;
        }
        Member currentOldest = oldestChangedBuffer.first();
        boolean selfIsOldest = currentOldest.uniqueAddress().equals(cluster.selfUniqueAddress());

        switch (state) {
            case START, YOUNGER -> {
                if (selfIsOldest && !selfExited) {
                    if (previousOldest == null || previousOldest.uniqueAddress().equals(cluster.selfUniqueAddress())) {
                        tryStartOldest();
                    } else {
                        state = State.BECOMING_OLDEST;
                        sendHandOverToMe();
                        scheduleHandOverRetry();
                    }
                } else {
                    previousOldest = currentOldest;
                }
            }
            case OLDEST -> {
                if (!selfIsOldest) {
                    newOldest = currentOldest;
                    state = State.WAS_OLDEST;
                    sendTakeOverFromMe();
                    scheduleTakeOverRetry();
                }
            }
            case BECOMING_OLDEST -> {
                if (!selfIsOldest) {
                    cancelRetry();
                    previousOldest = currentOldest;
                    state = State.YOUNGER;
                }
            }
            case WAS_OLDEST -> newOldest = currentOldest;
            case HANDING_OVER, END -> { }
        }
    }

    private void onHandOverToMe(ActorRef sender) {
        switch (state) {
            case OLDEST, WAS_OLDEST -> {
                cancelRetry();
                handOverTo.add(sender);
                sender.tell(HandOverInProgress.INSTANCE, getSelf());
                state = State.HANDING_OVER;
                if (singleton != null) {
                    log().info("Stopping singleton actor [{}] for hand-over to [{}]",
                            singleton.path(), sender.path().address());
                    singleton.tell(terminationMessage, getSelf());
                } else {
                    finishHandOver();
                }
            }
            case HANDING_OVER -> {
                handOverTo.add(sender);
                sender.tell(HandOverInProgress.INSTANCE, getSelf());
            }
            case YOUNGER -> sender.tell(HandOverDone.INSTANCE, getSelf());
            default -> { }
        }
    }

    private void onHandOverDone(ActorRef sender) {
        if (state == State.BECOMING_OLDEST) {
            cancelRetry();
            previousOldest = null;
            tryStartOldest();
        }
    }

    private void onTakeOverFromMe(ActorRef sender) {
        if (state == State.BECOMING_OLDEST || state == State.YOUNGER) {
            sender.tell(HandOverToMe.INSTANCE, getSelf());
        }
    }

    private void onTerminated(Terminated t) {
        if (!t.actor().equals(singleton)) {
            return;
        }
        log().info("Singleton actor [{}] was terminated", singleton.path());
        singleton = null;
        if (lease != null && holdingLease) {
            lease.release();
            holdingLease = false;
        }
        if (state == State.HANDING_OVER) {
            finishHandOver();
        } else if (state == State.OLDEST) {
            tryStartOldest();
        }
    }

    private void finishHandOver() {
        for (ActorRef peer : handOverTo) {
            peer.tell(HandOverDone.INSTANCE, getSelf());
        }
        handOverTo.clear();
        state = selfExited ? State.END : State.YOUNGER;
    }

    private void tryStartOldest() {
        if (singleton != null || state == State.END) {
            return;
        }
        if (lease != null && !holdingLease) {
            acquireLease();
            return;
        }
        state = State.OLDEST;
        log().info("Singleton manager starting singleton actor [{}]",
                getSelf().path().child(settings.singletonName()));
        singleton = getContext().actorOf(singletonProps, settings.singletonName());
        getContext().watch(singleton);
    }

    private void acquireLease() {
        if (acquiringLease) {
            return;
        }
        acquiringLease = true;
        ActorRef self = getSelf();
        lease.acquire(reason -> self.tell(new LeaseLost(reason), ActorRef.noSender))
                .whenComplete((acquired, err) -> {
                    boolean ok = err == null && Boolean.TRUE.equals(acquired);
                    if (err != null) {
                        log().warning("Failed to acquire singleton lease [{}]: {}",
                                lease.getSettings().leaseName(), err.getMessage());
                    }
                    self.tell(new AcquireLeaseResult(ok), ActorRef.noSender);
                });
    }

    private void onAcquireLeaseResult(AcquireLeaseResult result) {
        acquiringLease = false;
        if (result.acquired()) {
            holdingLease = true;
            tryStartOldest();
        } else {
            getContext().system().scheduler().scheduleOnce(
                    leaseUsageSettings.getLeaseRetryInterval(),
                    getSelf(),
                    RetryAcquireLeaseTick.INSTANCE,
                    getContext().dispatcher(),
                    getSelf());
        }
    }

    private void onLeaseLost(LeaseLost lost) {
        holdingLease = false;
        log().warning("Singleton lease [{}] lost: {}",
                lease.getSettings().leaseName(),
                lost.reason().map(Throwable::getMessage).orElse("unknown"));
        if (singleton != null) {
            singleton.tell(terminationMessage, getSelf());
        }
    }

    private void sendHandOverToMe() {
        if (previousOldest != null) {
            peerSelection(previousOldest.address()).tell(HandOverToMe.INSTANCE, getSelf());
        }
    }

    private void sendTakeOverFromMe() {
        if (newOldest != null) {
            peerSelection(newOldest.address()).tell(TakeOverFromMe.INSTANCE, getSelf());
        }
    }

    private ActorSelection peerSelection(Address address) {
        return getContext().actorSelection(address + getSelf().path().toStringWithoutAddress());
    }

    private void scheduleHandOverRetry() {
        cancelRetry();
        retryTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                settings.handOverRetryInterval(),
                settings.handOverRetryInterval(),
                getSelf(),
                HandOverRetryTick.INSTANCE,
                getContext().getDispatcher(),
                getSelf());
    }

    private void scheduleTakeOverRetry() {
        cancelRetry();
        retryTask = getContext().getSystem().scheduler().scheduleWithFixedDelay(
                settings.handOverRetryInterval(),
                settings.handOverRetryInterval(),
                getSelf(),
                TakeOverRetryTick.INSTANCE,
                getContext().getDispatcher(),
                getSelf());
    }

    private void cancelRetry() {
        if (retryTask != null) {
            retryTask.cancel();
            retryTask = null;
        }
    }
}
