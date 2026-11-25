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
 * Actor 的生命周期和消息接收协议。
 *
 * <p>ActorCell 在 actor creation、message processing、restart 和 termination 的对应阶段
 * 调用这些方法。应用代码通常继承 {@link AbstractActor}，并实现
 * {@link AbstractActor#createReceive()}。</p>
 */
public interface Actor {
    /**
     * 返回此 Actor 用于处理 child failure 的监督策略。
     *
     * @return supervisor strategy
     */
    default SupervisorStrategy supervisorStrategy() {
        return SupervisorStrategy.defaultStrategy;
    }

    /**
     * Actor 实例创建后、开始处理用户消息前调用。
     *
     * @throws Exception 初始化失败时抛出的异常
     */
    void preStart() throws Exception;

    /**
     * Actor 终止时调用，用于释放实例持有的资源。
     *
     * @throws Exception 释放资源失败时抛出的异常
     */
    void postStop() throws Exception;

    /**
     * 旧实例 restart 前调用。
     *
     * @param reason 触发 restart 的异常
     * @param message 触发失败的消息；初始化失败时为 {@code null}
     * @throws Exception restart 前置处理失败时抛出的异常
     */
    void preRestart(Throwable reason, Object message) throws Exception;

    /**
     * 新实例创建后调用，默认由 {@link AbstractActor} 转调 {@link #preStart()}。
     *
     * @param reason 触发 restart 的异常
     * @throws Exception restart 后置处理失败时抛出的异常
     */
    void postRestart(Throwable reason) throws Exception;

    /**
     * Receive 没有处理指定消息时调用。
     *
     * @param message 未匹配的用户消息
     */
    void unhandled(Object message);

    /**
     * 返回此 Actor 的初始 receive behavior。
     *
     * @return Actor 创建时使用的 receive behavior
     */
    AbstractActor.Receive receive();
}
