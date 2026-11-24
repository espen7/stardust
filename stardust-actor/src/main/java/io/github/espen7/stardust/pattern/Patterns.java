package io.github.espen7.stardust.pattern;

import io.github.espen7.stardust.actor.ActorPath;
import io.github.espen7.stardust.actor.ActorRef;

import java.time.Duration;
import java.util.concurrent.*;

/**
 * Actor interaction patterns 的 Java 静态入口。
 *
 * <p>当前实现提供 ask 请求应答模式。请求消息通过 ActorRef 正常进入接收方 Mailbox，
 * 请求方通过临时 PromiseActorRef 接收回复。</p>
 */
public final class Patterns {
    private Patterns() {
    }

    /**
     * 向 Actor 发送请求，并使用临时 PromiseActorRef 作为 sender 接收回复。
     * 请求结果与 timeout 竞争完成，只有第一个完成者生效。
     *
     * <p>当前实现为每个 ask 创建独立的计时器线程，临时 ActorPath 尚未注册到 ActorRefProvider。</p>
     *
     * @param actor   请求目标
     * @param message 请求消息
     * @param timeout 等待回复的最长时间，必须大于零
     * @return 回复完成时成功完成，超时时以 AskTimeoutException 失败完成
     * @throws IllegalArgumentException 当 timeout 为零或负数
     */
    public static CompletionStage<Object> ask(ActorRef actor, Object message, Duration timeout) {
        if (timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("Timeout length must be positive");
        CompletableFuture<Object> result = new CompletableFuture<>();
        PromiseActorRef promise = new PromiseActorRef(new ActorPath("/temp/" + System.nanoTime()), result);
        actor.tell(message, promise);
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        timer.schedule(() -> result.completeExceptionally(new AskTimeoutException("Ask timed out on " + actor)), timeout.toNanos(), TimeUnit.NANOSECONDS);
        result.whenComplete((value, error) -> timer.shutdown());
        return result;
    }

    /**
     * 使用毫秒指定等待时间的 ask 重载。
     *
     * @param actor         请求目标
     * @param message       请求消息
     * @param timeoutMillis 等待回复的时长，必须大于零
     * @return 请求回复或超时结果
     */
    public static CompletionStage<Object> ask(ActorRef actor, Object message, long timeoutMillis) {
        return ask(actor, message, Duration.ofMillis(timeoutMillis));
    }
}
