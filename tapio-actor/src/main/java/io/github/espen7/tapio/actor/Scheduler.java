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

import io.github.espen7.tapio.annotation.InternalApi;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link ActorSystem} 的一次性与周期调度入口。
 */
public abstract class Scheduler implements AutoCloseable {
    /**
     * 在初次延迟后重复执行任务，后续延迟从上次执行完成时开始计算。
     *
     * <p>任务不会重叠执行，也不补偿执行耗时或调度延迟。任务抛出异常后停止后续调度。
     * 原始任务实现 {@link SchedulerTask} 时，成功取消会转发 {@link SchedulerTask#cancelled()}。</p>
     *
     * @param initialDelay 初次延迟；非正时直接提交给 executor
     * @param delay 后续执行间隔，必须大于 0
     * @param runnable 到期任务
     * @param executor 执行任务的 Executor
     * @return 用于取消整个周期任务的句柄
     * @throws IllegalArgumentException 间隔非正或延迟超过调度器支持的最大值
     * @throws IllegalStateException 调度器已关闭
     */
    public Cancellable scheduleWithFixedDelay(Duration initialDelay, Duration delay,
                                              Runnable runnable, Executor executor) {
        return new AtomicCancellable(Cancellable.initialNotCancelled) {
            @Override
            protected Cancellable scheduledFirst() {
                AtomicCancellable self = this;
                return scheduleOnce(initialDelay, new SchedulerTask() {
                    @Override
                    public void run() {
                        try {
                            runnable.run();
                            if (self.get() != null) {
                                self.swap(scheduleOnce(delay, this, executor));
                            }
                        } catch (SchedulerException ignored) {
                        } catch (IllegalStateException error) {
                            if (!(error.getCause() instanceof SchedulerException)) throw error;
                        }
                    }

                    @Override
                    public void cancelled() {
                        if (runnable instanceof SchedulerTask task) task.cancelled();
                    }
                }, executor);
            }
        };
    }

    /**
     * 在初次延迟后重复向 Actor 投递消息，后续延迟从上次投递完成时开始计算。
     *
     * @param initialDelay 初次延迟；非正时直接提交给 executor
     * @param delay 后续投递间隔，必须大于 0
     * @param receiver 目标 Actor 引用
     * @param message 要投递的消息
     * @param executor 执行投递的 Executor
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @return 用于取消整个周期任务的句柄
     * @throws IllegalArgumentException 间隔非正或延迟超过调度器支持的最大值
     * @throws IllegalStateException 调度器已关闭
     */
    public final Cancellable scheduleWithFixedDelay(Duration initialDelay, Duration delay,
                                                    ActorRef receiver, Object message,
                                                    Executor executor, ActorRef sender) {
        return scheduleWithFixedDelay(initialDelay, delay, () -> {
            receiver.tell(message, sender);
            if (receiver instanceof InternalActorRef internal && internal.isTerminated()) throw new SchedulerException("timer active for terminated actor");
        }, executor);
    }

    /**
     * 按固定频率重复执行任务，并补偿错过的理想执行时间。
     *
     * <p>任务不会重叠执行；上次执行超过 interval 时，后续任务会尽快连续追补。
     * 任务抛出异常后停止后续调度。原始任务实现 {@link SchedulerTask} 时，成功取消会转发
     * {@link SchedulerTask#cancelled()}。</p>
     *
     * @param initialDelay 初次延迟；非正时直接提交给 executor
     * @param interval 理想执行间隔，必须大于 0
     * @param runnable 到期任务
     * @param executor 执行任务的 Executor
     * @return 用于取消整个周期任务的句柄
     * @throws IllegalArgumentException 间隔非正或延迟超过调度器支持的最大值
     * @throws IllegalStateException 调度器已关闭
     */
    public final Cancellable scheduleAtFixedRate(Duration initialDelay, Duration interval,
                                                 Runnable runnable, Executor executor) {
        return schedule(initialDelay, interval, runnable, executor);
    }

    /**
     * 按固定频率重复向 Actor 投递消息，并补偿错过的理想投递时间。
     *
     * @param initialDelay 初次延迟；非正时直接提交给 executor
     * @param interval 理想投递间隔，必须大于 0
     * @param receiver 目标 Actor 引用
     * @param message 要投递的消息
     * @param executor 执行投递的 Executor
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @return 用于取消整个周期任务的句柄
     * @throws IllegalArgumentException 间隔非正或延迟超过调度器支持的最大值
     * @throws IllegalStateException 调度器已关闭
     */
    public final Cancellable scheduleAtFixedRate(Duration initialDelay, Duration interval,
                                                 ActorRef receiver, Object message,
                                                 Executor executor, ActorRef sender) {
        return scheduleAtFixedRate(initialDelay, interval, () -> {
            receiver.tell(message, sender);
            if (receiver instanceof InternalActorRef internal && internal.isTerminated()) throw new SchedulerException("timer active for terminated actor");
        }, executor);
    }

    /**
     * INTERNAL API：由实现类提供 fixed-rate 调度。
     *
     * @param initialDelay 初次延迟
     * @param interval 理想执行间隔
     * @param runnable 到期任务
     * @param executor 执行任务的 Executor
     * @return 周期任务句柄
     */
    protected abstract Cancellable schedule(Duration initialDelay, Duration interval,
                                            Runnable runnable, Executor executor);

    /**
     * 调度一次任务，在触发任务的线程运行。
     *
     * @param delay 任务延迟。非正时在调用线程直接运行
     * @param runnable 到期任务
     * @return 用于取消尚未执行的任务
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public final Cancellable scheduleOnce(Duration delay, Runnable runnable) {
        return scheduleOnce(delay, runnable, Runnable::run);
    }

    /**
     * 在指定延迟后将任务交给 executor 执行。非正延迟直接提交给 executor。
     *
     * @param delay 任务延迟
     * @param runnable 到期任务
     * @param executor 执行任务的 Executor
     * @return 用于取消任务的句柄
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public abstract Cancellable scheduleOnce(Duration delay, Runnable runnable, Executor executor);

    /**
     * 在指定延迟后向 Actor 投递一条消息。
     *
     * @param delay 投递延迟。非正时在调用线程发起投递
     * @param receiver 目标 Actor 引用
     * @param message 要投递的消息
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @return 用于取消尚未执行的投递任务
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public final Cancellable scheduleOnce(Duration delay, ActorRef receiver, Object message, ActorRef sender) {
        return scheduleOnce(delay, receiver, message, Runnable::run, sender);
    }

    /**
     * 在指定延迟后将消息交给 executor 投递。
     *
     * @param delay 投递延迟；非正时直接提交给 executor
     * @param receiver 目标 Actor 引用
     * @param message 要投递的消息
     * @param executor 执行投递的 Executor
     * @param sender 消息发送者，可以为 {@link ActorRef#noSender}
     * @return 用于取消尚未执行的任务句柄
     * @throws IllegalStateException 调度器已关闭
     * @throws IllegalArgumentException 延迟大于调度器支持的最大值
     */
    public final Cancellable scheduleOnce(Duration delay, ActorRef receiver, Object message,
                                          Executor executor, ActorRef sender) {
        return scheduleOnce(delay, () -> receiver.tell(message, sender), executor);
    }

    /**
     * 返回调度器支持的最高周期任务频率，即 tick 粒度的倒数。
     *
     * @return 理论最高频率，单位为 Hz
     */
    public abstract double maxFrequency();

    /**
     * 关闭调度器并停止接收新任务。
     *
     * <p>仍待执行的 {@link TaskRunOnClose} 任务会在关闭时运行。
     */
    @Override
    public abstract void close();

    /**
     * 关闭调度器时仍需执行的 runtime 内部任务。
     */
    public interface TaskRunOnClose extends Runnable {
    }

    /**
     * INTERNAL API：原子保存当前周期任务，并以 null 表示已取消。
     */
    @InternalApi
    abstract static class AtomicCancellable extends AtomicReference<Cancellable> implements Cancellable {
        protected AtomicCancellable(Cancellable initialValue) {
            super(initialValue);
            try {
                compareAndSet(initialValue, scheduledFirst());
            } catch (SchedulerException error) {
                throw new IllegalStateException(error.getMessage(), error);
            }
        }

        protected abstract Cancellable scheduledFirst();

        protected final void swap(Cancellable replacement) {
            for (;;) {
                Cancellable current = get();
                if (current == null) {
                    if (replacement != null) replacement.cancel();
                    return;
                }
                if (compareAndSet(current, replacement)) return;
            }
        }

        @Override
        public final boolean cancel() {
            for (;;) {
                Cancellable current = get();
                if (current == null) return false;
                if (current.cancel()) return compareAndSet(current, null);
                if (compareAndSet(current, null)) return true;
            }
        }

        @Override
        public final boolean isCancelled() {
            return get() == null;
        }
    }
}
