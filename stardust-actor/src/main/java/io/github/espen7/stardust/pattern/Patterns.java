package io.github.espen7.stardust.pattern;

import io.github.espen7.stardust.actor.ActorRef;

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
     * <p>通过 {@link io.github.espen7.stardust.actor.Scheduler} 先注册超时任务，再发送请求。
     * 首个回复或超时决定结果。结果完成后，临时引用会停止，超时任务也会取消。
     *
     * <p>若目标已终止，仍会按普通消息投递路径发送请求，并立即以
     * {@link AskTimeoutException} 使返回的阶段失败。目标须为
     * {@link io.github.espen7.stardust.actor.InternalActorRef}。目标类型不受支持或存活目标的
     * {@code timeout} 非正时，返回的阶段以 {@link IllegalArgumentException} 失败。
     *
     * @param actor 请求目标
     * @param message 请求消息，须为非 {@code null}
     * @param timeout 回复时限，须为正数
     * @return 异步回复阶段。普通回复直接作为结果，
     *         {@link io.github.espen7.stardust.actor.Status.Success} 解包为结果，
     *         {@link io.github.espen7.stardust.actor.Status.Failure} 的原因使阶段失败。
     *         等待超时时以 {@link AskTimeoutException} 失败
     * @throws IllegalArgumentException 超时大于 Scheduler 支持的最大值
     * @throws IllegalStateException Scheduler 已关闭
     * @throws ArithmeticException 超时无法转换为 {@code long} 纳秒值
     * @throws NullPointerException 存活目标的超时为正数且请求消息为 {@code null}，或存活目标的超时为 {@code null}
     * @throws io.github.espen7.stardust.actor.InvalidMessageException 目标已终止且请求消息为 {@code null}
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
     * @throws io.github.espen7.stardust.actor.InvalidMessageException 目标已终止且请求消息为 {@code null}
     * @see #ask(ActorRef, Object, Duration)
     */
    public static CompletionStage<Object> ask(ActorRef actor, Object message, long timeoutMillis) {
        return ask(actor, message, Duration.ofMillis(timeoutMillis));
    }
}
