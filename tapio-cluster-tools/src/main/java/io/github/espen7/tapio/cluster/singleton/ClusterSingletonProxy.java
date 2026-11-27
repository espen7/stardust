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
import io.github.espen7.tapio.actor.ActorIdentity;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.Identify;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * 集群单例代理 Actor，自动追踪最老节点上的单例实例、缓存过渡期消息并转发业务请求。
 */
public final class ClusterSingletonProxy extends AbstractLoggingActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {

    private static final Comparator<Member> AGE_ORDERING = (a, b) -> {
        if (a.equals(b)) return 0;
        return a.isOlderThan(b) ? -1 : 1;
    };

    private enum IdentifyTick {
        INSTANCE
    }

    private record BufferedMessage(Object message, ActorRef sender) {
    }

    private final String singletonManagerPath;
    private final ClusterSingletonProxySettings settings;
    private final Cluster cluster;
    private final NavigableSet<Member> membersByAge = new TreeSet<>(AGE_ORDERING);
    private final Deque<BufferedMessage> buffer = new ArrayDeque<>();

    private ActorRef singleton;
    private Cancellable identifyTimer;
    private long identifyId = 0L;

    /**
     * 创建 {@link ClusterSingletonProxy} 的 {@link Props}。
     *
     * @param singletonManagerPath 单例管理器在节点上的相对路径（如 {@code /user/singletonManager}）
     * @param settings 代理配置
     * @return Props
     */
    public static Props props(String singletonManagerPath, ClusterSingletonProxySettings settings) {
        return Props.create(ClusterSingletonProxy.class,
                () -> new ClusterSingletonProxy(singletonManagerPath, settings));
    }

    private ClusterSingletonProxy(String singletonManagerPath, ClusterSingletonProxySettings settings) {
        this.singletonManagerPath = singletonManagerPath;
        this.settings = settings;
        this.cluster = Cluster.get(getContext().system());
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.SubscriptionInitialStateMode.InitialStateAsEvents,
                ClusterEvent.MemberEvent.class);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        if (identifyTimer != null) {
            identifyTimer.cancel();
        }
        cluster.unsubscribe(getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.MemberUp.class, e -> track(e.member()))
                .match(ClusterEvent.MemberExited.class, e -> untrack(e.member()))
                .match(ClusterEvent.MemberDowned.class, e -> untrack(e.member()))
                .match(ClusterEvent.MemberRemoved.class, e -> untrack(e.member()))
                .match(IdentifyTick.class, ignored -> identify())
                .match(ActorIdentity.class, id -> {
                    if (id.getActorRef().isPresent() && id.correlationId() instanceof Long cid && cid == identifyId) {
                        singleton = id.getActorRef().get();
                        getContext().watch(singleton);
                        cancelIdentify();
                        flushBuffer();
                    }
                })
                .match(Terminated.class, t -> {
                    if (t.actor().equals(singleton)) {
                        singleton = null;
                        identify();
                    }
                })
                .matchAny(msg -> {
                    if (singleton != null) {
                        singleton.tell(msg, getSender());
                    } else {
                        if (settings.bufferSize() > 0) {
                            if (buffer.size() >= settings.bufferSize()) {
                                buffer.removeFirst();
                            }
                            buffer.addLast(new BufferedMessage(msg, getSender()));
                        }
                    }
                })
                .build();
    }

    private boolean matchesRole(Member m) {
        return settings.role().map(m::hasRole).orElse(true);
    }

    private void track(Member m) {
        if (!matchesRole(m)) {
            return;
        }
        Member prevOldest = membersByAge.isEmpty() ? null : membersByAge.first();
        membersByAge.remove(m);
        membersByAge.add(m);
        Member newOldest = membersByAge.first();
        if (!newOldest.equals(prevOldest)) {
            singleton = null;
            identify();
        }
    }

    private void untrack(Member m) {
        if (!matchesRole(m)) {
            return;
        }
        Member prevOldest = membersByAge.isEmpty() ? null : membersByAge.first();
        membersByAge.remove(m);
        if (m.status() == MemberStatus.Up) {
            membersByAge.add(m);
        }
        Member newOldest = membersByAge.isEmpty() ? null : membersByAge.first();
        if (newOldest == null) {
            singleton = null;
            cancelIdentify();
        } else if (!newOldest.equals(prevOldest)) {
            singleton = null;
            identify();
        }
    }

    private void identify() {
        if (membersByAge.isEmpty()) {
            return;
        }
        Address oldestAddr = membersByAge.first().address();
        String path = oldestAddr + singletonManagerPath + "/" + settings.singletonName();
        long cid = ++identifyId;
        ActorSelection sel = getContext().actorSelection(path);
        sel.tell(new Identify(cid), getSelf());
        if (identifyTimer == null) {
            identifyTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                    settings.singletonIdentificationInterval(),
                    settings.singletonIdentificationInterval(),
                    getSelf(),
                    IdentifyTick.INSTANCE,
                    getContext().dispatcher(),
                    getSelf());
        }
    }

    private void cancelIdentify() {
        if (identifyTimer != null) {
            identifyTimer.cancel();
            identifyTimer = null;
        }
    }

    private void flushBuffer() {
        while (!buffer.isEmpty() && singleton != null) {
            BufferedMessage bm = buffer.removeFirst();
            singleton.tell(bm.message(), bm.sender());
        }
    }
}
