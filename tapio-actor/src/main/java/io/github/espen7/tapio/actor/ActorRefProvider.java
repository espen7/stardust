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

/**
 * ActorRef 和 ActorPath 的创建边界，对应 Pekko ActorRefProvider。
 */
public abstract class ActorRefProvider {

    /** 创建 Provider 抽象实例。 */
    protected ActorRefProvider() {
    }

    /**
     * 在 parent 下创建 ActorRef。
     *
     * @param props Actor 创建配置
     * @param name child name
     * @param parent parent ActorRef
     * @return 新 ActorRef
     */
    public abstract ActorRef actorOf(Props props, String name, ActorRef parent);

    /**
     * 返回 user guardian。
     *
     * @return user guardian 引用
     */
    public abstract ActorRef rootGuardian();

    /**
     * 返回 system guardian。
     *
     * @return system guardian 引用
     */
    public abstract ActorRef systemGuardian();

    /**
     * 创建挂在 system guardian 下的系统 Actor。
     *
     * @param props Actor 创建配置
     * @param name system guardian 下唯一的 child name
     * @return 新系统 ActorRef
     */
    public abstract ActorRef systemActorOf(Props props, String name);

    /**
     * 返回 dead letters 引用。
     *
     * @return dead letters ActorRef
     */
    public abstract ActorRef deadLetters();
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
}
