package io.github.espen7.stardust.actor;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * ActorSystem 使用的调度入口，支持一次性 Runnable 和 Actor 消息投递。
 */
public class Scheduler implements AutoCloseable {

    // 单独的 timer thread 只负责触发任务，Actor 消息仍通过 ActorRef.tell 进入 Mailbox。
    private final ScheduledExecutorService executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

    /**
     * 在 delay 到期后运行任务，返回的 Cancellable 可取消尚未执行的任务。
     */
    public Cancellable scheduleOnce(Duration delay, Runnable runnable) {
        ScheduledFuture<?> future = executor.schedule(runnable, delay.toNanos(), TimeUnit.NANOSECONDS);
        return new Cancellable() {
            @Override
            public boolean cancel() {
                return future.cancel(false);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }
        };
    }

    /**
     * 在 delay 到期后向 receiver 投递 message。
     */
    public Cancellable scheduleOnce(Duration delay, ActorRef receiver, Object message, ActorRef sender) {
        ScheduledFuture<?> future = executor.schedule(() -> receiver.tell(message, sender), delay.toNanos(), TimeUnit.NANOSECONDS);
        return new Cancellable() {
            public boolean cancel() {
                return future.cancel(false);
            }

            public boolean isCancelled() {
                return future.isCancelled();
            }
        };
    }

    /**
     * 停止接收新任务，并让已提交的任务按 ScheduledExecutorService 规则收尾。
     */
    @Override
    public void close() {
        executor.shutdown();
    }
}
