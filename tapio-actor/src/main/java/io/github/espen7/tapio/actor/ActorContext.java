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

import io.github.espen7.tapio.japi.pf.PartialFunction;

import java.time.Duration;
import java.util.Optional;

/**
 * Actor 执行期间可访问的上下文。
 *
 * <p>ActorContext 暴露子 Actor 创建、停止、watch 和 behavior 切换操作。它不是
 * thread-safe API，调用者应在 Actor message processing thread 中使用。</p>
 */
public interface ActorContext extends ActorRefFactory {
    /**
     * 返回所属 ActorSystem。
     *
     * @return 当前 Actor 所属的 ActorSystem
     */
    ActorSystem system();

    /**
     * 返回当前 Actor 的引用。
     *
     * @return 当前 ActorRef
     */
    ActorRef self();

    /**
     * 返回当前 Envelope 中的 sender。
     *
     * @return 当前消息 sender；没有 sender 时返回 dead letters 引用
     */
    ActorRef sender();

    /**
     * 建立 DeathWatch 关系。
     *
     * @param actor 要观察的 ActorRef
     * @return 被观察的 ActorRef
     */
    ActorRef watch(ActorRef actor);

    /**
     * 建立 DeathWatch 关系，并在目标终止时投递自定义消息。
     *
     * @param actor 要观察的 ActorRef
     * @param message 目标终止时投递的消息；可以为 {@code null}
     * @return 被观察的 ActorRef
     */
    ActorRef watchWith(ActorRef actor, Object message);

    /**
     * 返回当前 receive timeout；未设置时返回 {@code null}。
     *
     * @return 当前 receive timeout
     */
    Duration receiveTimeout();

    /**
     * 设置静默后投递 {@link ReceiveTimeout} 的时长。
     *
     * @param timeout 静默时长，null 对应未设置
     */
    void setReceiveTimeout(Duration timeout);

    /**
     * 解除 DeathWatch 关系。
     *
     * @param actor 要解除观察的 ActorRef
     * @return 被解除观察的 ActorRef
     */
    ActorRef unwatch(ActorRef actor);

    /**
     * 替换当前 behavior。
     *
     * @param behavior 新的 receive behavior
     */
    void become(PartialFunction<Object, Void> behavior);

    /**
     * 根据 discardOld 决定替换或压入当前 behavior。
     *
     * @param behavior 新的 receive behavior
     * @param discardOld 是否丢弃当前 behavior
     */
    void become(PartialFunction<Object, Void> behavior, boolean discardOld);

    /**
     * 弹出当前 behavior，恢复前一个 behavior。
     */
    void unbecome();

    /** @return 当前 Actor 的 children */
    Iterable<ActorRef> children();

    /** @return 指定名称的 child */
    Optional<ActorRef> child(String name);

    /** @return 当前 Actor 的监督者 */
    ActorRef parent();

    /** @return 创建当前 Actor 使用的 Props */
    Props props();
}
