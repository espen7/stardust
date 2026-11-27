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
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.actor.AbstractLoggingActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.AddressTerminated;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.Deploy;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;
import io.github.espen7.tapio.dispatch.sysmsg.DeathWatchNotification;
import io.github.espen7.tapio.event.AddressTerminatedTopic;

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

/**
 * INTERNAL API：监控被 Watch 的远端节点健康状态并在不可达时触发 {@link AddressTerminated}。
 */
@InternalApi
public class RemoteWatcher extends AbstractLoggingActor implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    /**
     * 创建 {@link RemoteWatcher} 的 {@link Props}。
     *
     * @param settings 远程配置
     * @param failureDetector 失效检测器注册表
     * @return Props
     */
    public static Props props(RemoteSettings settings, FailureDetectorRegistry<Address> failureDetector) {
        return Props.create(
                        RemoteWatcher.class,
                        () -> new RemoteWatcher(
                                failureDetector,
                                settings.WatchHeartBeatInterval,
                                settings.WatchUnreachableReaperInterval,
                                settings.WatchHeartbeatExpectedResponseAfter))
                .withDispatcher(Dispatchers.InternalDispatcherId)
                .withDeploy(Deploy.local);
    }

    /** 开始远程监控请求。 */
    public record WatchRemote(InternalActorRef watchee, InternalActorRef watcher) {
    }

    /** 取消远程监控请求。 */
    public record UnwatchRemote(InternalActorRef watchee, InternalActorRef watcher) {
    }

    /** 远程监控心跳请求。 */
    public static final class Heartbeat implements HeartbeatMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        /** 单例实例。 */
        public static final Heartbeat INSTANCE = new Heartbeat();

        private Heartbeat() {
        }

        @Serial
        private Object readResolve() {
            return INSTANCE;
        }
    }

    /** 远程监控心跳响应。 */
    public record HeartbeatRsp(int addressUid) implements HeartbeatMessage, Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    private enum HeartbeatTick {
        INSTANCE
    }

    private enum ReapUnreachableTick {
        INSTANCE
    }

    private record ExpectedFirstHeartbeat(Address from) {
    }

    /** 监控统计快照（供测试查询）。 */
    public record Stats(int watching, int watchingNodes) {
        public static final Stats EMPTY = new Stats(0, 0);
    }

    /** 查询 {@link Stats} 的消息。 */
    public static final class GetStats {
        /** 单例实例。 */
        public static final GetStats INSTANCE = new GetStats();

        private GetStats() {
        }
    }

    protected final FailureDetectorRegistry<Address> failureDetector;
    private final Duration heartbeatInterval;
    private final Duration unreachableReaperInterval;
    private final Duration heartbeatExpectedResponseAfter;
    private final RemoteActorRefProvider remoteProvider;
    private final HeartbeatRsp selfHeartbeatRspMsg;

    private final Map<InternalActorRef, Set<InternalActorRef>> watching = new HashMap<>();
    private final Map<Address, Set<InternalActorRef>> watcheeByNodes = new HashMap<>();
    private final Set<Address> unreachable = new HashSet<>();
    private final Map<Address, Long> addressUids = new HashMap<>();

    private final Cancellable heartbeatTask;
    private final Cancellable failureDetectorReaperTask;

    /**
     * 构造 {@link RemoteWatcher}。
     *
     * @param failureDetector 失效检测器注册表
     * @param heartbeatInterval 心跳间隔
     * @param unreachableReaperInterval 不可达扫描间隔
     * @param heartbeatExpectedResponseAfter 首次心跳预期响应等待时长
     */
    public RemoteWatcher(
            FailureDetectorRegistry<Address> failureDetector,
            Duration heartbeatInterval,
            Duration unreachableReaperInterval,
            Duration heartbeatExpectedResponseAfter) {
        this.failureDetector = failureDetector;
        this.heartbeatInterval = heartbeatInterval;
        this.unreachableReaperInterval = unreachableReaperInterval;
        this.heartbeatExpectedResponseAfter = heartbeatExpectedResponseAfter;
        this.remoteProvider = (RemoteActorRefProvider) ((ExtendedActorSystem) getContext().system()).provider();
        int uid = AddressUidExtension.INSTANCE.get(getContext().system()).addressUid();
        this.selfHeartbeatRspMsg = new HeartbeatRsp(uid);
        this.heartbeatTask = getContext().system().scheduler().scheduleWithFixedDelay(
                heartbeatInterval, heartbeatInterval, getSelf(), HeartbeatTick.INSTANCE,
                getContext().dispatcher(), getSelf());
        this.failureDetectorReaperTask = getContext().system().scheduler().scheduleWithFixedDelay(
                unreachableReaperInterval, unreachableReaperInterval, getSelf(), ReapUnreachableTick.INSTANCE,
                getContext().dispatcher(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        heartbeatTask.cancel();
        failureDetectorReaperTask.cancel();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HeartbeatTick.class, ignored -> sendHeartbeat())
                .match(Heartbeat.class, ignored -> getSender().tell(selfHeartbeatRspMsg, getSelf()))
                .match(HeartbeatRsp.class, rsp -> receiveHeartbeatRsp(rsp.addressUid()))
                .match(ReapUnreachableTick.class, ignored -> reapUnreachable())
                .match(ExpectedFirstHeartbeat.class, msg -> triggerFirstHeartbeat(msg.from()))
                .match(WatchRemote.class, w -> addWatch(w.watchee(), w.watcher()))
                .match(UnwatchRemote.class, u -> removeWatch(u.watchee(), u.watcher()))
                .match(Terminated.class, t -> terminated((InternalActorRef) t.actor(), t.existenceConfirmed(), t.addressTerminated()))
                .match(GetStats.class, ignored -> {
                    int total = watching.values().stream().mapToInt(Set::size).sum();
                    getSender().tell(new Stats(total, watcheeByNodes.size()), getSelf());
                })
                .build();
    }

    private void receiveHeartbeatRsp(long uid) {
        Address from = getSender().path().address();
        if (failureDetector.isMonitoring(from)) {
            log().debug("Received heartbeat rsp from [{}]", from);
        } else {
            log().debug("Received first heartbeat rsp from [{}]", from);
        }
        if (watcheeByNodes.containsKey(from) && !unreachable.contains(from)) {
            Long prevUid = addressUids.get(from);
            if (prevUid == null || prevUid != uid) {
                reWatch(from);
            }
            addressUids.put(from, uid);
            failureDetector.heartbeat(from);
        }
    }

    private void reapUnreachable() {
        for (Address a : new ArrayList<>(watcheeByNodes.keySet())) {
            if (!unreachable.contains(a) && !failureDetector.isAvailable(a)) {
                log().warning("Detected unreachable: [{}]", a);
                remoteProvider.quarantine(
                        a, Optional.ofNullable(addressUids.get(a)), "Deemed unreachable by remote failure detector");
                publishAddressTerminated(a);
                unreachable.add(a);
            }
        }
    }

    protected void publishAddressTerminated(Address address) {
        log().debug("Publish AddressTerminated [{}]", address);
        AddressTerminatedTopic.INSTANCE.get(getContext().system()).publish(new AddressTerminated(address));
    }

    protected boolean shouldWatch(InternalActorRef watchee) {
        return remoteProvider.remoteSettings().UseUnsafeRemoteFeaturesWithoutCluster;
    }

    private void addWatch(InternalActorRef watchee, InternalActorRef watcher) {
        if (!shouldWatch(watchee)) {
            remoteProvider.warnIfUnsafeDeathwatchWithoutCluster(watchee, watcher, "Watch");
            return;
        }
        watching.computeIfAbsent(watchee, ignored -> new HashSet<>()).add(watcher);
        Address watcheeAddr = watchee.path().address();
        if (!watcheeByNodes.containsKey(watcheeAddr) && unreachable.contains(watcheeAddr)) {
            unreachable.remove(watcheeAddr);
            failureDetector.remove(watcheeAddr);
        }
        watcheeByNodes.computeIfAbsent(watcheeAddr, ignored -> new HashSet<>()).add(watchee);
        getContext().watch(watchee);
    }

    private void removeWatch(InternalActorRef watchee, InternalActorRef watcher) {
        if (!shouldWatch(watchee)) {
            return;
        }
        Set<InternalActorRef> watchers = watching.get(watchee);
        if (watchers != null) {
            watchers.remove(watcher);
            if (watchers.isEmpty()) {
                getContext().unwatch(watchee);
                removeWatchee(watchee);
            }
        }
    }

    private void terminated(InternalActorRef watchee, boolean existenceConfirmed, boolean addressTerminated) {
        Set<InternalActorRef> watchers = watching.get(watchee);
        if (watchers != null) {
            for (InternalActorRef watcher : new ArrayList<>(watchers)) {
                watcher.sendSystemMessage(new DeathWatchNotification(watchee, existenceConfirmed, addressTerminated));
            }
        }
        removeWatchee(watchee);
    }

    private void removeWatchee(InternalActorRef watchee) {
        watching.remove(watchee);
        Address addr = watchee.path().address();
        Set<InternalActorRef> set = watcheeByNodes.get(addr);
        if (set != null) {
            set.remove(watchee);
            if (set.isEmpty()) {
                watcheeByNodes.remove(addr);
                failureDetector.remove(addr);
                unreachable.remove(addr);
                addressUids.remove(addr);
            }
        }
    }

    private void sendHeartbeat() {
        for (Address a : watcheeByNodes.keySet()) {
            if (!unreachable.contains(a)) {
                if (!failureDetector.isMonitoring(a)) {
                    log().debug("Sending first heartbeat to [{}]", a);
                    getContext().system().scheduler().scheduleOnce(
                            heartbeatExpectedResponseAfter,
                            getSelf(),
                            new ExpectedFirstHeartbeat(a),
                            getContext().dispatcher(),
                            getSelf());
                } else {
                    log().debug("Sending heartbeat to [{}]", a);
                }
                ActorSelection sel = getContext().actorSelection(getSelf().path().toStringWithAddress(a));
                sel.tell(Heartbeat.INSTANCE, getSelf());
            }
        }
    }

    private void triggerFirstHeartbeat(Address address) {
        if (watcheeByNodes.containsKey(address) && !failureDetector.isMonitoring(address)) {
            log().debug("Trigger extra expected heartbeat from [{}]", address);
            failureDetector.heartbeat(address);
        }
    }

    private void reWatch(Address address) {
        Set<InternalActorRef> watchees = watcheeByNodes.get(address);
        if (watchees != null) {
            for (InternalActorRef watchee : watchees) {
                getContext().unwatch(watchee);
                getContext().watch(watchee);
            }
        }
    }
}
