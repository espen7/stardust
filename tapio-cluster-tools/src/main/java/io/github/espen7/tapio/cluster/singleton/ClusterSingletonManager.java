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
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
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
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

/**
 * 在集群中最老的匹配节点上托管唯一单例 Actor，并在主节点离开或下线时自动迁移。
 */
public final class ClusterSingletonManager extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    private static final Comparator<Member> AGE_ORDERING = (a, b) -> {
        if (a.equals(b)) return 0;
        return a.isOlderThan(b) ? -1 : 1;
    };

    private record AcquireLeaseResult(boolean acquired) {
    }

    private enum RetryAcquireLeaseTick {
        INSTANCE
    }

    private record LeaseLost(Optional<Throwable> reason) {
    }

    private final Props singletonProps;
    private final Object terminationMessage;
    private final ClusterSingletonManagerSettings settings;
    private final Cluster cluster;
    private final NavigableSet<Member> oldestChangedBuffer = new TreeSet<>(AGE_ORDERING);
    private final Lease lease;
    private final LeaseUsageSettings leaseUsageSettings;

    private ActorRef singleton;
    private boolean stopping = false;
    private boolean selfUp = false;
    private boolean acquiringLease = false;
    private boolean holdingLease = false;

    /**
     * 创建 {@link ClusterSingletonManager} 的 {@link Props}。
     *
     * @param singletonProps 单例 Actor 创建配置
     * @param terminationMessage 优雅迁移时发送给旧单例的停止消息
     * @param settings 管理器配置
     * @return Props
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
                .match(ClusterEvent.MemberExited.class, e -> removeOrUpdateMember(e.member()))
                .match(ClusterEvent.MemberDowned.class, e -> removeOrUpdateMember(e.member()))
                .match(ClusterEvent.MemberRemoved.class, e -> removeOrUpdateMember(e.member()))
                .match(AcquireLeaseResult.class, this::onAcquireLeaseResult)
                .match(RetryAcquireLeaseTick.class, ignored -> evaluateOldest())
                .match(LeaseLost.class, this::onLeaseLost)
                .match(Terminated.class, t -> {
                    if (t.actor().equals(singleton)) {
                        log().info("Singleton actor [{}] was terminated", singleton.path());
                        singleton = null;
                        stopping = false;
                        if (lease != null && holdingLease) {
                            lease.release();
                            holdingLease = false;
                        }
                        evaluateOldest();
                    }
                })
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

    private void evaluateOldest() {
        if (!selfUp || oldestChangedBuffer.isEmpty()) {
            return;
        }
        Member oldest = oldestChangedBuffer.first();
        boolean selfIsOldest = oldest.uniqueAddress().equals(cluster.selfUniqueAddress());
        if (selfIsOldest && singleton == null && !stopping) {
            if (lease != null && !holdingLease) {
                acquireLease();
                return;
            }
            startSingleton();
        } else if (!selfIsOldest && singleton != null && !stopping) {
            stopping = true;
            log().info("Stopping singleton actor [{}] as node is no longer oldest", singleton.path());
            singleton.tell(terminationMessage, getSelf());
        }
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
            evaluateOldest();
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
        if (singleton != null && !stopping) {
            stopping = true;
            singleton.tell(terminationMessage, getSelf());
        }
    }

    private void startSingleton() {
        log().info("Singleton manager starting singleton actor [{}]",
                getSelf().path().child(settings.singletonName()));
        singleton = getContext().actorOf(singletonProps, settings.singletonName());
        getContext().watch(singleton);
    }
}
