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
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;
import io.github.espen7.tapio.remote.FailureDetectorRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * INTERNAL API：向集群成员环上的邻居节点周期性发送心跳并更新失效检测器。
 */
@InternalApi
public final class ClusterHeartbeatSender extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    /** 通知发送器当前活跃成员列表变更。 */
    public record UpdateHeartbeatTargets(Set<UniqueAddress> members) {
    }

    private enum HeartbeatTick {
        INSTANCE
    }

    private record ExpectedFirstHeartbeat(UniqueAddress from) {
    }

    private final ClusterSettings settings;
    private final UniqueAddress selfUniqueAddress;
    private final FailureDetectorRegistry<Address> failureDetector;
    private final Cancellable heartbeatTask;
    private final AtomicLong seqNr = new AtomicLong(0L);
    private Set<UniqueAddress> activeTargets = Set.of();

    /**
     * 构造心跳发送器。
     *
     * @param settings 集群配置
     * @param selfUniqueAddress 本节点唯一地址
     * @param failureDetector 失效检测器注册表
     */
    public ClusterHeartbeatSender(
            ClusterSettings settings,
            UniqueAddress selfUniqueAddress,
            FailureDetectorRegistry<Address> failureDetector) {
        this.settings = settings;
        this.selfUniqueAddress = selfUniqueAddress;
        this.failureDetector = failureDetector;
        this.heartbeatTask = getContext().system().scheduler().scheduleWithFixedDelay(
                settings.HeartbeatInterval,
                settings.HeartbeatInterval,
                getSelf(),
                HeartbeatTick.INSTANCE,
                getContext().dispatcher(),
                getSelf());
    }

    /** 创建 {@link Props}。 */
    public static Props props(
            ClusterSettings settings,
            UniqueAddress selfUniqueAddress,
            FailureDetectorRegistry<Address> failureDetector) {
        return Props.create(ClusterHeartbeatSender.class,
                () -> new ClusterHeartbeatSender(settings, selfUniqueAddress, failureDetector));
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        heartbeatTask.cancel();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HeartbeatTick.class, ignored -> sendHeartbeats())
                .match(ClusterMessages.HeartbeatRsp.class, rsp -> {
                    if (activeTargets.contains(rsp.from())) {
                        failureDetector.heartbeat(rsp.from().address());
                    }
                })
                .match(ExpectedFirstHeartbeat.class, msg -> {
                    if (activeTargets.contains(msg.from()) && !failureDetector.isMonitoring(msg.from().address())) {
                        failureDetector.heartbeat(msg.from().address());
                    }
                })
                .match(UpdateHeartbeatTargets.class, msg -> updateTargets(msg.members()))
                .build();
    }

    private void updateTargets(Set<UniqueAddress> allMembers) {
        List<UniqueAddress> sorted = new ArrayList<>(allMembers);
        Collections.sort(sorted);
        int selfIdx = sorted.indexOf(selfUniqueAddress);
        Set<UniqueAddress> next = new HashSet<>();
        if (selfIdx >= 0 && sorted.size() > 1) {
            int count = Math.min(settings.MonitoredByNrOfMembers, sorted.size() - 1);
            for (int i = 1; i <= count; i++) {
                next.add(sorted.get((selfIdx + i) % sorted.size()));
            }
        }
        for (UniqueAddress old : activeTargets) {
            if (!next.contains(old)) {
                failureDetector.remove(old.address());
            }
        }
        this.activeTargets = Collections.unmodifiableSet(next);
    }

    private void sendHeartbeats() {
        long seq = seqNr.incrementAndGet();
        long now = System.nanoTime();
        for (UniqueAddress target : activeTargets) {
            if (!failureDetector.isMonitoring(target.address())) {
                getContext().system().scheduler().scheduleOnce(
                        settings.HeartbeatExpectedResponseAfter,
                        getSelf(),
                        new ExpectedFirstHeartbeat(target),
                        getContext().dispatcher(),
                        getSelf());
            }
            ActorSelection sel = getContext().actorSelection(
                    target.address() + "/system/cluster-heartbeatReceiver");
            sel.tell(new ClusterMessages.Heartbeat(selfUniqueAddress.address(), seq, now), getSelf());
        }
    }
}
