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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSelection;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * 提供 Classic Actor 交互模式的 Java 静态入口。
 */
public final class Patterns {
    private Patterns() {
    }

    /**
     * 向 Actor 异步发送请求，并使用临时 {@link PromiseActorRef} 作为发送者接收回复。
     *
     * <p>通过 {@link io.github.espen7.tapio.actor.Scheduler} 先注册超时任务，再发送请求。
     * 首个回复或超时决定结果。结果完成后，临时引用会停止，超时任务也会取消。
     *
     * <p>若目标已终止，仍会按普通消息投递路径发送请求，并立即以
     * {@link AskTimeoutException} 使返回的阶段失败。目标须为
     * {@link io.github.espen7.tapio.actor.InternalActorRef}。目标类型不受支持或存活目标的
     * {@code timeout} 非正时，返回的阶段以 {@link IllegalArgumentException} 失败。
     *
     * @param actor 请求目标
     * @param message 请求消息，须为非 {@code null}
     * @param timeout 回复时限，须为正数
     * @return 异步回复阶段。普通回复直接作为结果，
     *         {@link io.github.espen7.tapio.actor.Status.Success} 解包为结果，
     *         {@link io.github.espen7.tapio.actor.Status.Failure} 的原因使阶段失败。
     *         等待超时时以 {@link AskTimeoutException} 失败
     * @throws IllegalArgumentException 超时大于 Scheduler 支持的最大值
     * @throws IllegalStateException Scheduler 已关闭
     * @throws ArithmeticException 超时无法转换为 {@code long} 纳秒值
     * @throws NullPointerException 存活目标的超时为正数且请求消息为 {@code null}，或存活目标的超时为 {@code null}
     * @throws io.github.espen7.tapio.actor.InvalidMessageException 目标已终止且请求消息为 {@code null}
     */
    public static CompletionStage<Object> ask(ActorRef actor, Object message, Duration timeout) {
        return new AskableActorRef(actor).internalAsk(message, timeout, ActorRef.noSender);
    }

    /**
     * 向 Actor 异步发送请求，以毫秒指定回复时限。
     *
     * @param actor 请求目标
     * @param message 请求消息，须为非 {@code null}
     * @param timeoutMillis 回复时限（毫秒），须为正数
     * @return 与 {@link #ask(ActorRef, Object, Duration)} 相同的异步回复阶段
     * @throws IllegalArgumentException 超时大于 Scheduler 支持的最大值
     * @throws IllegalStateException Scheduler 已关闭
     * @throws ArithmeticException 超时无法转换为 {@code long} 纳秒值
     * @throws NullPointerException 存活目标的超时为正数且请求消息为 {@code null}
     * @throws io.github.espen7.tapio.actor.InvalidMessageException 目标已终止且请求消息为 {@code null}
     * @see #ask(ActorRef, Object, Duration)
     */
    public static CompletionStage<Object> ask(ActorRef actor, Object message, long timeoutMillis) {
        return ask(actor, message, Duration.ofMillis(timeoutMillis));
    }

    /**
     * 向选择中的 Actor 发送请求，使用首个到达的回复完成结果。
     *
     * @param selection 请求目标
     * @param message 请求消息
     * @param timeout 正数回复时限
     * @return 首个回复或超时的结果
     */
    public static CompletionStage<Object> ask(ActorSelection selection, Object message, Duration timeout) {
        return new AskableActorSelection(selection).internalAsk(message, timeout, ActorRef.noSender);
    }
}
