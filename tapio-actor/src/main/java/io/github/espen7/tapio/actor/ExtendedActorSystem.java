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

import io.github.espen7.tapio.event.LoggingFilter;

import java.util.concurrent.ThreadFactory;

/**
 * 暴露给内部组件和扩展使用的 ActorSystem 内部契约，对应 Pekko {@code ExtendedActorSystem}。
 */
public abstract class ExtendedActorSystem extends ActorSystem {
    /**
     * 创建挂在 system guardian 下的系统 Actor。
     *
     * @param props Actor 创建配置
     * @param name system guardian 下唯一的 child name
     * @return 新系统 ActorRef
     */
    public abstract ActorRef systemActorOf(Props props, String name);

    /**
     * 返回此系统共享的线程工厂。
     *
     * @return 此 ActorSystem 的线程工厂
     */
    public abstract ThreadFactory threadFactory();

    /**
     * 返回此系统的 ActorRefProvider。
     *
     * @return ActorRefProvider
     */
    public abstract ActorRefProvider provider();

    /**
     * 返回此系统内部用于反射加载与实例化的 {@link DynamicAccess}。
     *
     * @return DynamicAccess 实例
     */
    public abstract DynamicAccess dynamicAccess();

    /**
     * 返回此系统使用的 {@link LoggingFilter}。
     *
     * @return LoggingFilter 实例
     */
    public abstract LoggingFilter logFilter();
}
