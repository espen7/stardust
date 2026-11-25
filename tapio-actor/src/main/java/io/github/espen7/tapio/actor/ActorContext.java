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

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Actor 执行期间可访问的上下文。
 *
 * <p>ActorContext 暴露子 Actor 创建、停止、watch 和 behavior 切换操作。它不是
 * thread-safe API，调用者应在 Actor message processing thread 中使用。</p>
 */
public interface ActorContext {
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
     * 使用自动生成的 name 创建 child。
     *
     * @param props child 的创建配置
     * @return 新 child 的 ActorRef
     */
    ActorRef actorOf(Props props);

    /**
     * 使用指定 name 创建 child。
     *
     * @param props child 的创建配置
     * @param name parent 下唯一的 child name
     * @return 新 child 的 ActorRef
     */
    ActorRef actorOf(Props props, String name);

    /**
     * 异步停止目标 Actor。
     *
     * @param actor 要停止的 ActorRef
     */
    void stop(ActorRef actor);

    /**
     * 建立 DeathWatch 关系。
     *
     * @param actor 要观察的 ActorRef
     * @return 被观察的 ActorRef
     */
    ActorRef watch(ActorRef actor);

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
    void become(AbstractActor.Receive behavior);

    /**
     * 根据 discardOld 决定替换或压入当前 behavior。
     *
     * @param behavior 新的 receive behavior
     * @param discardOld 是否丢弃当前 behavior
     */
    void become(AbstractActor.Receive behavior, boolean discardOld);

    /**
     * 弹出当前 behavior，恢复前一个 behavior。
     */
    void unbecome();

    /**
     * 返回当前 Actor 的 child 引用快照。
     *
     * @return child 引用的不可变快照
     */
    Collection<ActorRef> getChildren();

    /**
     * 返回 ActorSystem 终止完成时的 stage。
     *
     * @return ActorSystem 终止完成通知
     */
    default CompletionStage<Terminated> getWhenTerminated() {
        return system().getWhenTerminated();
    }
}
