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

import com.typesafe.config.Config;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.Extension;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.remote.AddressUidExtension;
import io.github.espen7.tapio.remote.DefaultFailureDetectorRegistry;
import io.github.espen7.tapio.remote.FailureDetector;
import io.github.espen7.tapio.remote.FailureDetectorRegistry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tapio 集群扩展入口，提供集群成员管理、事件订阅、加入/离开/Down 等核心 API。
 */
public final class Cluster implements Extension {

    /**
     * {@link Cluster} 扩展 ID。
     */
    public static final ClusterExtension INSTANCE = new ClusterExtension();

    /**
     * 扩展提供者实现。
     */
    public static final class ClusterExtension extends AbstractExtensionId<Cluster> implements ExtensionIdProvider {
        private ClusterExtension() {
        }

        @Override
        public ExtensionId<Cluster> lookup() {
            return INSTANCE;
        }

        @Override
        public Cluster createExtension(ExtendedActorSystem system) {
            return new Cluster(system);
        }
    }

    /**
     * 获取给定 {@link ActorSystem} 的 {@link Cluster} 扩展实例。
     *
     * @param system ActorSystem
     * @return {@link Cluster} 实例
     */
    public static Cluster get(ActorSystem system) {
        return INSTANCE.apply(system);
    }

    private final ExtendedActorSystem system;
    private final ClusterSettings settings;
    private final UniqueAddress selfUniqueAddress;
    private final ClusterReadView readView;
    private final FailureDetectorRegistry<Address> failureDetector;
    private final ActorRef clusterCoreDaemon;
    private final AtomicBoolean terminating = new AtomicBoolean(false);

    private Cluster(ExtendedActorSystem system) {
        this.system = system;
        this.settings = new ClusterSettings(system.settings().config(), system.settings().name());
        Address defaultAddr = system.provider().getDefaultAddress();
        long uid = AddressUidExtension.INSTANCE.get(system).longAddressUid();
        this.selfUniqueAddress = new UniqueAddress(defaultAddr, uid);
        this.readView = new ClusterReadView(selfUniqueAddress, settings.Roles, settings.AppVersion);
        this.failureDetector = new DefaultFailureDetectorRegistry<>(() -> {
            try {
                return system.dynamicAccess().createInstanceFor(
                        settings.FailureDetectorImplementationClass,
                        FailureDetector.class,
                        List.of(Map.entry(Config.class, settings.FailureDetectorConfig),
                                Map.entry(EventStream.class, system.eventStream())));
            } catch (Throwable t) {
                throw new ConfigurationException(
                        "Cannot instantiate FailureDetector [" + settings.FailureDetectorImplementationClass + "]", t);
            }
        });
        system.systemActorOf(ClusterHeartbeatReceiver.props(selfUniqueAddress), "cluster-heartbeatReceiver");
        this.clusterCoreDaemon = system.systemActorOf(
                ClusterDaemon.props(settings, selfUniqueAddress, readView, failureDetector),
                "cluster-core-daemon");
        if (!settings.SeedNodes.isEmpty()) {
            joinSeedNodes(settings.SeedNodes);
        }
    }

    /** 返回集群配置。 */
    public ClusterSettings settings() {
        return settings;
    }

    /** 返回本节点唯一地址。 */
    public UniqueAddress selfUniqueAddress() {
        return selfUniqueAddress;
    }

    /** 返回本节点通信地址。 */
    public Address selfAddress() {
        return selfUniqueAddress.address();
    }

    /** 返回本节点角色集合。 */
    public Set<String> selfRoles() {
        return settings.Roles;
    }

    /** 返回本节点数据中心名称。 */
    public String selfDataCenter() {
        return settings.SelfDataCenter;
    }

    /** 返回本节点当前 {@link Member} 快照。 */
    public Member selfMember() {
        return readView.selfMember();
    }

    /** 返回当前集群状态快照。 */
    public ClusterEvent.CurrentClusterState state() {
        return readView.state();
    }

    /** INTERNAL API：返回集群只读视图。 */
    @io.github.espen7.tapio.annotation.InternalApi
    public ClusterReadView readView() {
        return readView;
    }

    /** 返回集群失效检测器注册表。 */
    public FailureDetectorRegistry<Address> failureDetector() {
        return failureDetector;
    }

    /** 判断集群扩展是否正在关闭。 */
    public boolean isTerminated() {
        return terminating.get();
    }

    /**
     * 订阅集群领域事件（默认使用 {@link ClusterEvent.SubscriptionInitialStateMode#InitialStateAsSnapshot}）。
     *
     * @param subscriber 订阅者 ActorRef
     * @param to 订阅的事件类型列表
     */
    public void subscribe(ActorRef subscriber, Class<?>... to) {
        subscribe(subscriber, ClusterEvent.SubscriptionInitialStateMode.InitialStateAsSnapshot, to);
    }

    /**
     * 订阅集群领域事件。
     *
     * @param subscriber 订阅者 ActorRef
     * @param initialStateMode 初始状态推送模式
     * @param to 订阅的事件类型列表
     */
    public void subscribe(
            ActorRef subscriber,
            ClusterEvent.SubscriptionInitialStateMode initialStateMode,
            Class<?>... to) {
        clusterCoreDaemon.tell(new ClusterDaemon.Subscribe(subscriber, initialStateMode, Set.of(to)), ActorRef.noSender);
    }

    /**
     * 退订所有集群领域事件。
     *
     * @param subscriber 订阅者 ActorRef
     */
    public void unsubscribe(ActorRef subscriber) {
        clusterCoreDaemon.tell(new ClusterDaemon.Unsubscribe(subscriber, Optional.empty()), ActorRef.noSender);
    }

    /**
     * 退订指定类型的集群领域事件。
     *
     * @param subscriber 订阅者 ActorRef
     * @param to 事件类型
     */
    public void unsubscribe(ActorRef subscriber, Class<?> to) {
        clusterCoreDaemon.tell(new ClusterDaemon.Unsubscribe(subscriber, Optional.of(to)), ActorRef.noSender);
    }

    /**
     * 向指定节点发送当前集群状态快照。
     *
     * @param receiver 接收方 ActorRef
     */
    public void sendCurrentClusterState(ActorRef receiver) {
        clusterCoreDaemon.tell(new ClusterDaemon.SendCurrentClusterState(receiver), ActorRef.noSender);
    }

    /**
     * 加入指定地址所在的集群。
     *
     * @param address 目标集群节点地址
     */
    public void join(Address address) {
        clusterCoreDaemon.tell(new ClusterMessages.JoinTo(address), ActorRef.noSender);
    }

    /**
     * 按种子节点列表加入集群。
     *
     * @param seedNodes 种子节点地址列表
     */
    public void joinSeedNodes(List<Address> seedNodes) {
        clusterCoreDaemon.tell(new ClusterMessages.JoinSeedNodes(List.copyOf(seedNodes)), ActorRef.noSender);
    }

    /**
     * 请求将指定地址的节点优雅移出集群。
     *
     * @param address 目标节点地址
     */
    public void leave(Address address) {
        clusterCoreDaemon.tell(new ClusterMessages.Leave(address), ActorRef.noSender);
    }

    /**
     * 将指定地址的节点标记为 {@link MemberStatus#Down}。
     *
     * @param address 目标节点地址
     */
    public void down(Address address) {
        clusterCoreDaemon.tell(new ClusterMessages.Down(address), ActorRef.noSender);
    }

    /**
     * 注册本节点成为 {@link MemberStatus#Up} 时的回调。
     *
     * @param callback 回调逻辑
     */
    public void registerOnMemberUp(Runnable callback) {
        clusterCoreDaemon.tell(new ClusterDaemon.RegisterOnMemberUp(callback), ActorRef.noSender);
    }
}
