/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.actor;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.github.espen7.tapio.annotation.DoNotInherit;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.serialization.Serialization;

/**
 * ActorRef 和 ActorPath 的创建边界。
 */
@DoNotInherit
public abstract class ActorRefProvider {

    /** 创建 Provider 抽象实例。 */
    protected ActorRefProvider() {
    }

    /**
     * 返回位于根路径 {@code /} 的 root guardian。
     *
     * @return 监督 user guardian 和 system guardian 的引用
     */
    public abstract InternalActorRef rootGuardian();

    /**
     * 返回指定地址处的根引用。
     *
     * @param address 目标地址
     * @return 本地地址返回根引用，本地 Provider 对其他地址返回死信引用
     */
    public abstract ActorRef rootGuardianAt(Address address);

    /**
     * 返回位于 {@code /user} 的 user guardian。
     *
     * @return 监督应用顶层 Actor 的引用
     */
    public abstract LocalActorRef guardian();

    /**
     * 返回 system guardian。
     *
     * @return system guardian 引用
     */
    public abstract LocalActorRef systemGuardian();

    /**
     * 返回 dead letters 引用。
     *
     * @return dead letters ActorRef
     */
    public abstract ActorRef deadLetters();

    @InternalApi
    public abstract ActorRef ignoreRef();

    /**
     * 返回此 ActorSystem 的根路径。
     *
     * @return 本地地址下的根路径
     */
    public abstract ActorPath rootPath();

    /**
     * 返回 Provider 所使用的系统配置。
     *
     * @return 当前 ActorSystem 的有效配置
     */
    public abstract ActorSystem.Settings settings();

    /**
     * INTERNAL API：初始化 Provider。
     *
     * @param system 所属 ActorSystem
     */
    @InternalApi
    public abstract void init(ActorSystemImpl system);

    /**
     * 返回此 Provider 使用的部署配置索引。
     *
     * @return 部署配置索引
     */
    public abstract Deployer deployer();

    /**
     * 返回调度器。
     *
     * @return 所属 ActorSystem 的共享 {@link Scheduler}
     */
    public abstract Scheduler scheduler();
    /**
     * 分配无自定义前缀的临时路径。
     *
     * @return 尚未注册的临时路径
     */
    public ActorPath tempPath() {
        return tempPath("");
    }

    /**
     * 分配在当前系统内唯一的临时路径。
     *
     * @param prefix 临时路径最后一段的前缀，可以为空字符串
     * @return 尚未注册的临时路径
     */
    public abstract ActorPath tempPath(String prefix);
    /**
     * 返回临时引用的逻辑父级。
     *
     * @return 存放已注册临时引用的容器
     */
    public abstract InternalActorRef tempContainer();

    /**
     * 注册内部引用，使其可以在临时容器中按路径名称查找。
     *
     * @param actorRef 要注册的内部引用
     * @param path 由此 Provider 的 {@link #tempPath(String)} 分配的路径
     */
    public abstract void registerTempActor(InternalActorRef actorRef, ActorPath path);

    /**
     * 注销临时容器中的引用。
     *
     * @param path 此 Provider 此前注册的临时路径
     */
    public abstract void unregisterTempActor(ActorPath path);

    /**
     * 为调用方分配的路径创建引用。调用方负责注册子引用并启动。
     *
     * @param system 所属 ActorSystem
     * @param props Actor 创建配置
     * @param supervisor 监督该 Actor 的引用
     * @param path 已分配路径及 incarnation UID
     * @param systemService 是否为本地系统服务
     * @param deploy 关闭部署查找时使用的部署配置
     * @param lookupDeploy 是否按路径查找部署配置
     * @param async 是否由 supervisor 异步激活外部挂载的 Actor
     * @return 已初始化的内部引用
     */
    @InternalApi
    public abstract InternalActorRef actorOf(ActorSystemImpl system, Props props, InternalActorRef supervisor,
                                             ActorPath path, boolean systemService, Optional<Deploy> deploy,
                                             boolean lookupDeploy, boolean async);

    /**
     * 从绝对路径字符串解析 ActorRef。字符串可包含 incarnation UID。
     *
     * @param path 带 ActorSystem 地址的路径
     * @return 活跃引用、代表指定路径的空引用或系统死信引用
     */
    public abstract ActorRef resolveActorRef(String path);

    /**
     * 从结构化路径解析 ActorRef。
     *
     * @param path 待查询的路径
     * @return 活跃引用、代表指定路径的空引用或系统死信引用
     */
    public abstract ActorRef resolveActorRef(ActorPath path);

    /**
     * 返回 Provider 终止时完成的通知。
     *
     * @return 以 root guardian 的终止通知完成的 stage
     */
    public abstract CompletionStage<Terminated> terminationFuture();

    /**
     * 返回可用于到达目标地址的本地地址。
     *
     * @param address 目标地址
     * @return 本地 Provider 只为本系统地址返回结果
     */
    public abstract Optional<Address> getExternalAddressFor(Address address);

    /**
     * 返回 Provider 的默认通信地址。
     *
     * @return 本地 Provider 返回本系统地址
     */
    public abstract Address getDefaultAddress();

    @InternalApi
    public abstract Serialization.Information serializationInformation();

    @InternalApi
    public abstract String addressString();

    @InternalApi
    public abstract long systemUid();
}
