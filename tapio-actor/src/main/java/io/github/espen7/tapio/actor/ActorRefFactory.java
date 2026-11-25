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

import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.dispatch.Dispatcher;

/**
 * ActorSystem 和 ActorContext 共用的 Actor 创建与停止入口。
 *
 * <p>ActorSystem 将应用 Actor 挂到 user guardian，ActorContext 将子 Actor 挂到当前 Actor。
 * 通过 ActorContext 使用时，应在当前 Actor 的消息处理线程调用。</p>
 */
public interface ActorRefFactory {
    /**
     * 返回此 factory 关联的 Dispatcher。
     *
     * @return ActorSystem 的默认 Dispatcher，或当前 Actor 使用的 Dispatcher
     */
    Dispatcher dispatcher();

    /**
     * 使用自动生成的名称创建 Actor。
     *
     * @param props Actor 的创建配置
     * @return 新 Actor 的引用，Actor 的初始化异步执行
     * @throws ConfigurationException 部署、Dispatcher 或 Mailbox 配置无效
     * @throws IllegalStateException 父 Actor 正在终止或已终止
     */
    ActorRef actorOf(Props props);

    /**
     * 使用指定名称创建 Actor。
     *
     * @param props Actor 的创建配置
     * @param name 父 Actor 下唯一的合法名称，不能以 {@code $} 开头
     * @return 新 Actor 的引用，Actor 的初始化异步执行
     * @throws InvalidActorNameException 名称无效或已被使用
     * @throws ConfigurationException 部署、Dispatcher 或 Mailbox 配置无效
     * @throws IllegalStateException 父 Actor 正在终止或已终止
     */
    ActorRef actorOf(Props props, String name);

    /**
     * 构造相对或绝对路径的选择。匹配在消息投递时进行。
     *
     * @param path 路径文本，可含通配符
     * @return 从对应锚点开始的选择
     */
    ActorSelection actorSelection(String path);

    /**
     * 构造带地址的路径选择。匹配在消息投递时进行。
     *
     * @param path 待选择的路径
     * @return 从指定地址开始的选择
     */
    ActorSelection actorSelection(ActorPath path);

    /**
     * 请求异步停止 Actor。Actor 停止自身时，当前消息处理仍会执行完毕。
     *
     * @param actor 要停止的引用
     */
    void stop(ActorRef actor);
}
