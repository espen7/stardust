package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.event.EventStream;
import io.github.espen7.stardust.event.LoggingAdapter;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

/**
 * ActorSystem 是一个独立 Actor runtime 的顶层入口和资源所有者。
 *
 * <p>每个 ActorSystem 拥有自己的 actor hierarchy、Dispatcher、Scheduler 和
 * ActorRefProvider。顶层 Actor 挂在 user guardian 下，子 Actor 则由其 parent 的
 * {@link ActorContext} 创建。ActorSystem 关闭时会停止 guardian，并在运行时资源
 * 完成关闭后完成 {@link #getWhenTerminated()} 返回的 stage。</p>
 *
 * <p>创建系统后，应使用 {@link #terminate()} 或 {@link #close()} 释放它持有的线程资源。
 * ActorSystem 本身不提供同步等待接口，调用者可以等待返回的
 * {@link CompletionStage}。</p>
 */
public abstract class ActorSystem implements AutoCloseable {
    /**
     * 创建一个使用默认名称 {@code default} 的本地 ActorSystem。
     *
     * @return 新建的 ActorSystem。调用者负责在使用完成后关闭它
     */
    public static ActorSystem create() {
        return create("default");
    }

    /**
     * 创建一个指定 system name 的本地 ActorSystem。
     *
     * <p>名称用于组成该系统的 ActorPath，例如
     * {@code pekko://<name>/user/<actor-name>}。</p>
     *
     * @param name system name
     * @return 新建的 ActorSystem
     * @throws IllegalArgumentException 当名称无效时
     */
    public static ActorSystem create(String name) {
        return new ActorSystemImpl(name);
    }

    /**
     * 在 user guardian 下创建顶层 Actor。
     *
     * <p>创建会注册 child 名称并返回其 ActorRef。Actor 实例的初始化通过 mailbox
     * 异步执行，因此方法返回不表示 {@code preStart()} 已经完成。</p>
     *
     * @param props Actor 实例的创建配置
     * @param name parent 下唯一的 Actor 名称
     * @return 新 Actor 的引用
     * @throws IllegalArgumentException 当名称无效或已被占用时
     * @throws IllegalStateException 当 ActorSystem 正在关闭时
     */
    public abstract ActorRef actorOf(Props props, String name);

    /**
     * 返回该系统的 dead letters 引用。
     *
     * <p>向 dead letters 发送的消息不会交给 Actor 处理。当前实现吸收这些消息，
     * 尚未发布 Pekko 的 DeadLetter event。</p>
     *
     * @return 此 ActorSystem 的 dead letters 引用
     */
    public abstract ActorRef deadLetters();

    /**
     * 返回此系统共享的 Dispatcher。
     *
     * <p>Actor 的 Mailbox 由 Dispatcher 调度。应用代码通常不需要直接操作它。</p>
     *
     * @return 此系统的 Dispatcher
     */
    public abstract Dispatcher dispatcher();

    /**
     * 返回此系统共享的 Scheduler。
     *
     * <p>Scheduler 用于延迟执行 Runnable 或延迟向 ActorRef 投递消息。关闭
     * ActorSystem 时 Scheduler 会停止接收新任务。</p>
     *
     * @return 此系统的 Scheduler
     */
    public abstract Scheduler scheduler();

    /**
     * 返回此系统的事件总线。
     *
     * <p>日志以结构化 {@link io.github.espen7.stardust.event.LogEvent} 发布到此处；
     * 应用也可以订阅事件观察 Actor runtime 的行为。</p>
     *
     * @return 此 ActorSystem 独有的 EventStream
     */
    public abstract EventStream eventStream();

    /**
     * 返回带有此 ActorSystem source 信息的日志门面。
     *
     * @return 可发布到 EventStream 的日志适配器
     */
    public abstract LoggingAdapter log();

    /**
     * 异步停止 ActorSystem。
     *
     * <p>该调用发起 root guardian 的终止流程，并立即返回完成通知。通知会在
     * guardian 终止以及 Scheduler、Dispatcher 关闭后完成。重复调用会返回同一个
     * 终止 stage。</p>
     *
     * @return ActorSystem 终止完成时的 stage，其结果描述终止的 guardian
     */
    public abstract CompletionStage<Terminated> terminate();

    /**
     * 返回 ActorSystem 的终止完成通知。
     *
     * <p>系统运行期间该 stage 保持未完成，调用 {@link #terminate()} 后，在关闭流程
     * 完成时正常完成。</p>
     *
     * @return ActorSystem 终止完成时的 stage
     */
    public abstract CompletionStage<Terminated> getWhenTerminated();

    /**
     * 发起 ActorSystem 关闭。
     *
     * <p>此方法满足 {@link AutoCloseable}，其行为等同于调用 {@link #terminate()}，
     * 但不会等待异步关闭完成。需要等待时，请等待 {@link #getWhenTerminated()}。</p>
     */
    @Override
    public void close() {
        terminate();
    }

    /**
     * 安排一个一次性任务。
     *
     * <p>任务到期后在 Scheduler 的执行线程上运行。返回的句柄可用于取消尚未执行的任务。</p>
     *
     * @param delay 从调用到执行之间的延迟。零值或负值会尽快执行
     * @param runnable 到期后执行的任务
     * @return 可取消的任务句柄
     * @throws java.util.concurrent.RejectedExecutionException 当 Scheduler 已关闭时
     */
    public Cancellable scheduleOnce(Duration delay, Runnable runnable) {
        return scheduler().scheduleOnce(delay, runnable);
    }
}
