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
package io.github.espen7.tapio.cluster.sbr;

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.coordination.lease.Lease;
import io.github.espen7.tapio.coordination.lease.LeaseProvider;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * INTERNAL API：脑裂解决器 Actor，委托 {@link DowningStrategy} 执行稳定窗口后的脑裂裁决。
 */
@InternalApi
public final class SplitBrainResolver extends AbstractLoggingActor {

    private enum DecideTick {
        INSTANCE
    }

    private enum AcquireLeaseTick {
        INSTANCE
    }

    private record AcquireLeaseResult(boolean holdingLease) {
    }

    /**
     * 从系统配置构造 {@link SplitBrainResolver} {@link Props}。
     */
    public static Props props(ActorSystem system) {
        SplitBrainResolverSettings settings = new SplitBrainResolverSettings(system.settings().config());
        return props(settings.downingStableAfter(), settings.createStrategy());
    }

    /**
     * 使用显式稳定时间与策略构造 {@link SplitBrainResolver} {@link Props}。
     */
    public static Props props(Duration stableAfter, DowningStrategy strategy) {
        return Props.create(SplitBrainResolver.class, () -> new SplitBrainResolver(stableAfter, strategy));
    }

    private final Cluster cluster;
    private final Duration stableAfter;
    private final DowningStrategy strategy;
    private final Lease lease;

    private final Set<Member> members = new HashSet<>();
    private final Set<Member> unreachable = new HashSet<>();
    private Address leader;
    private Cancellable decideTimer;
    private boolean acquiringLease = false;

    public SplitBrainResolver(Duration stableAfter, DowningStrategy strategy) {
        this.cluster = Cluster.get(getContext().getSystem());
        this.stableAfter = stableAfter;
        this.strategy = strategy;
        if (strategy instanceof LeaseMajority lm) {
            String leaseName = getContext().getSystem().settings().name() + "-tapio-sbr";
            this.lease = LeaseProvider.get(getContext().getSystem())
                    .getLease(leaseName, lm.leaseImplementation(), cluster.selfAddress().toString());
        } else {
            this.lease = null;
        }
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(),
                ClusterEvent.MemberEvent.class,
                ClusterEvent.ReachabilityEvent.class,
                ClusterEvent.LeaderChanged.class);
    }

    @Override
    public void postStop() {
        if (decideTimer != null) {
            decideTimer.cancel();
        }
        cluster.unsubscribe(getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.CurrentClusterState.class, state -> {
                    members.clear();
                    members.addAll(state.members());
                    unreachable.clear();
                    unreachable.addAll(state.unreachable());
                    leader = state.leader().orElse(null);
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.MemberUp.class, ev -> {
                    members.remove(ev.member());
                    members.add(ev.member());
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.MemberRemoved.class, ev -> {
                    members.remove(ev.member());
                    unreachable.remove(ev.member());
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.MemberEvent.class, ev -> {
                    members.remove(ev.member());
                    members.add(ev.member());
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.UnreachableMember.class, ev -> {
                    unreachable.add(ev.member());
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.ReachableMember.class, ev -> {
                    unreachable.remove(ev.member());
                    rescheduleIfNeeded();
                })
                .match(ClusterEvent.LeaderChanged.class, ev -> leader = ev.leader().orElse(null))
                .match(DecideTick.class, tick -> decide())
                .match(AcquireLeaseTick.class, tick -> acquireLeaseNow())
                .match(AcquireLeaseResult.class, this::onAcquireLeaseResult)
                .build();
    }

    private void rescheduleIfNeeded() {
        if (decideTimer != null) {
            decideTimer.cancel();
            decideTimer = null;
        }
        if (!unreachable.isEmpty()) {
            decideTimer = getContext().getSystem().scheduler().scheduleOnce(
                    stableAfter,
                    getSelf(),
                    DecideTick.INSTANCE,
                    getContext().getDispatcher(),
                    getSelf());
        }
    }

    private void decide() {
        if (unreachable.isEmpty()) {
            return;
        }
        if (leader != null && !leader.equals(cluster.selfAddress())) {
            return;
        }
        strategy.setSeenBy(cluster.state().seenBy());
        strategy.setReachability(cluster.readView().latestGossip().overview().reachability());
        if (strategy instanceof LeaseMajority lm) {
            if (acquiringLease) {
                return;
            }
            acquiringLease = true;
            int reachableCount = (int) members.stream().filter(m -> !unreachable.contains(m)).count();
            int unreachableCount = unreachable.size();
            if (lm.isInMinority(reachableCount, unreachableCount)
                    && !lm.acquireLeaseDelayForMinority().isZero()) {
                getContext().getSystem().scheduler().scheduleOnce(
                        lm.acquireLeaseDelayForMinority(),
                        getSelf(),
                        AcquireLeaseTick.INSTANCE,
                        getContext().getDispatcher(),
                        getSelf());
            } else {
                acquireLeaseNow();
            }
            return;
        }
        List<Member> toDown = strategy.decideAndSelect(members, unreachable);
        for (Member m : toDown) {
            log().warning("SBR [{}] downing [{}]", strategy.getClass().getSimpleName(), m.address());
            cluster.down(m.address());
        }
    }

    private void acquireLeaseNow() {
        if (lease == null || unreachable.isEmpty()) {
            acquiringLease = false;
            return;
        }
        ActorRef self = getSelf();
        lease.acquire().whenComplete((acquired, err) -> {
            boolean ok = err == null && Boolean.TRUE.equals(acquired);
            if (err != null) {
                log().warning("SBR [LeaseMajority] failed to acquire lease: {}", err.getMessage());
            }
            self.tell(new AcquireLeaseResult(ok), ActorRef.noSender);
        });
    }

    private void onAcquireLeaseResult(AcquireLeaseResult result) {
        acquiringLease = false;
        if (unreachable.isEmpty()) {
            if (result.holdingLease() && lease != null) {
                lease.release();
            }
            return;
        }
        if (result.holdingLease()) {
            for (Member m : List.copyOf(unreachable)) {
                log().warning("SBR [LeaseMajority] acquired lease, downing unreachable [{}]", m.address());
                cluster.down(m.address());
            }
            if (lease != null) {
                lease.release();
            }
        } else {
            for (Member m : List.copyOf(members)) {
                if (!unreachable.contains(m)) {
                    log().warning("SBR [LeaseMajority] did not acquire lease, downing reachable [{}]", m.address());
                    cluster.down(m.address());
                }
            }
        }
    }
}
