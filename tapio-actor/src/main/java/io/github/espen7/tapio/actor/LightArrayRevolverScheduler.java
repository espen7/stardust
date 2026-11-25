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

import com.typesafe.config.Config;
import io.github.espen7.tapio.dispatch.AbstractNodeQueue;
import io.github.espen7.tapio.dispatch.ExecutionContext;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 按 tick 触发一次性任务的时间轮调度器。
 *
 * <p>单个定时线程推进时间轮，并将到期任务提交给各自的 {@link Executor}。
 */
public class LightArrayRevolverScheduler extends Scheduler {
    private static final boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");
    private static final Runnable CancelledTask = () -> {};
    private static final Runnable ExecutedTask = () -> {};
    private static final Cancellable NotCancellable = new Cancellable() {
        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };

    private final int wheelSize;
    private final Duration shutdownTimeout;
    private final LoggingAdapter log;
    private final ThreadFactory threadFactory;
    private final long start;
    private final long tickNanos;
    private final int wheelMask;
    private final TaskQueue queue = new TaskQueue();
    private final AtomicReference<CompletableFuture<List<TaskHolder>>> stopped = new AtomicReference<>();
    private volatile Thread timerThread;

    /**
     * 使用 ActorSystem 配置创建并启动时间轮。
     *
     * @param config 包含 tapio.scheduler 的配置
     * @param log 调度器记录错误和配置警告的日志适配器
     * @param threadFactory 创建 timer thread 的工厂
     * @throws IllegalArgumentException 时间轮参数未通过检查
     */
    public LightArrayRevolverScheduler(Config config, LoggingAdapter log, ThreadFactory threadFactory) {
        this.log = log;
        this.threadFactory = threadFactory;
        wheelSize = config.getInt(ActorPath.Scheme + ".scheduler.ticks-per-wheel");
        if ((wheelSize & (wheelSize - 1)) != 0) {
            throw new IllegalArgumentException("requirement failed: ticks-per-wheel must be a power of 2");
        }
        long tickMillis = config.getDuration(ActorPath.Scheme + ".scheduler.tick-duration").toMillis();
        long minimum = windows ? 10 : 1;
        if (tickMillis < minimum) {
            String message = "requirement failed: minimum supported tapio.scheduler.tick-duration"
                    + (windows ? " on Windows" : "") + " is " + minimum + "ms";
            if (config.getBoolean(ActorPath.Scheme + ".scheduler.error-on-tick-duration-verification-failed")) {
                throw new IllegalArgumentException(message);
            }
            log.warning(message + ", adjusted to " + minimum + "ms now.");
            tickMillis = minimum;
        }
        shutdownTimeout = Duration.ofMillis(config.getDuration(ActorPath.Scheme + ".scheduler.shutdown-timeout").toMillis());
        start = clock();
        tickNanos = TimeUnit.MILLISECONDS.toNanos(tickMillis);
        wheelMask = wheelSize - 1;
        timerThread = threadFactory.newThread(new Timer());
        timerThread.start();
    }

    /**
     * 读取用于计算相对时间的单调时钟。
     *
     * @return 当前时钟读数，单位为纳秒
     */
    protected long clock() {
        return System.nanoTime();
    }

    /**
     * 返回时间轮启动时的 tick 编号，可在测试中覆写。
     *
     * @return 起始 tick 编号，默认为 {@code 0}
     */
    protected int startTick() {
        return 0;
    }

    /**
     * 返回等待定时线程停止并交回剩余任务的最长时间。
     *
     * @return {@code tapio.scheduler.shutdown-timeout} 指定的时限，默认为 5 秒
     */
    protected Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * 暂停定时线程，等待后续 tick。被中断时保留线程的中断状态。
     *
     * @param nanos 等待时间，实际休眠粒度取决于操作系统
     */
    protected void waitNanos(long nanos) {
        long sleepMillis = windows ? (nanos + 4_999_999) / 10_000_000 * 10 : (nanos + 999_999) / 1_000_000;
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    private long roundUp(long nanos) {
        long rounded = ((nanos - 1) / tickNanos + 1) * tickNanos;
        return rounded > 0 && nanos > 0 ? rounded : nanos;
    }

    @Override
    public Cancellable scheduleOnce(Duration delay, Runnable runnable, Executor executor) {
        try {
            return schedule(executor, runnable, roundUp(delay.toNanos()));
        } catch (SchedulerException error) {
            throw new IllegalStateException(error.getMessage(), error);
        }
    }

    private Cancellable schedule(Executor executor, Runnable runnable, long delayNanos) {
        Objects.requireNonNull(runnable, "runnable");
        Objects.requireNonNull(executor, "executor");
        if (delayNanos <= 0) {
            if (stopped.get() != null) throw new SchedulerException("cannot enqueue after timer shutdown");
            executor.execute(runnable);
            return NotCancellable;
        }
        if (stopped.get() != null) throw new SchedulerException("cannot enqueue after timer shutdown");
        checkMaxDelay(delayNanos);
        TaskHolder task = new TaskHolder(runnable, (int) (delayNanos / tickNanos), executor);
        queue.add(task);
        if (stopped.get() != null && task.cancel()) {
            throw new SchedulerException("cannot enqueue after timer shutdown");
        }
        return task;
    }

    private void checkMaxDelay(long delayNanos) {
        if (delayNanos / tickNanos > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Task scheduled with [" + TimeUnit.NANOSECONDS.toSeconds(delayNanos)
                    + "] seconds delay, which is too far in future, maximum delay is ["
                    + (TimeUnit.NANOSECONDS.toSeconds(tickNanos * Integer.MAX_VALUE) - 1) + "] seconds");
        }
    }

    private CompletableFuture<List<TaskHolder>> stop() {
        CompletableFuture<List<TaskHolder>> tasks = new CompletableFuture<>();
        return stopped.compareAndSet(null, tasks) ? tasks : CompletableFuture.completedFuture(List.of());
    }

    /**
     * 停止时间轮，并在调用线程执行仍待运行的 {@link TaskRunOnClose} 任务。
     *
     * @throws IllegalStateException 等待时间轮停止超时、被中断或失败
     */
    @Override
    public void close() {
        try {
            for (TaskHolder holder : stop().get(getShutdownTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
                if (holder.task instanceof TaskRunOnClose) runTask(holder);
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException(error);
        }
    }

    private void runTask(Runnable task) {
        try {
            task.run();
        } catch (SchedulerException ignored) {
            /* 关闭时的任务可能尝试继续调度。 */
        } catch (Throwable error) {
            ActorCell.rethrowIfFatal(error);
            log.error(error, "exception while executing timer task");
        }
    }

    private final class Timer implements Runnable {
        private int tick = startTick();
        private long totalTick = tick;
        private final TaskQueue[] wheel = new TaskQueue[wheelSize];
        private TaskQueue spareTaskQueue = new TaskQueue();

        private Timer() {
            for (int i = 0; i < wheel.length; i++) wheel[i] = new TaskQueue();
        }

        private List<TaskHolder> clearAll() {
            List<TaskHolder> tasks = new ArrayList<>();
            for (TaskQueue bucket : wheel) collect(bucket, tasks);
            collect(queue, tasks);
            return tasks;
        }

        private void collect(TaskQueue source, List<TaskHolder> tasks) {
            TaskHolder task;
            while ((task = source.poll()) != null) tasks.add(task);
        }

        private void checkQueue(long time) {
            AbstractNodeQueue.Node<TaskHolder> node;
            while ((node = queue.pollNode()) != null) {
                int ticks = node.value.ticks;
                if (ticks == 0) {
                    node.value.executeTask();
                } else {
                    int futureTick = (int) ((time - start + ticks * tickNanos + tickNanos - 1) / tickNanos);
                    node.value.ticks = futureTick - tick;
                    wheel[futureTick & wheelMask].addNode(node);
                }
            }
        }

        @Override
        public void run() {
            try {
                nextTick();
            } catch (Throwable error) {
                log.error(error, "exception on LARS’ timer thread");
                CompletableFuture<List<TaskHolder>> stop = stopped.get();
                if (stop == null) {
                    Thread thread = threadFactory.newThread(this);
                    log.info("starting new LARS thread");
                    try {
                        thread.start();
                    } catch (Throwable failure) {
                        log.error(failure, "LARS cannot start new thread, ship’s going down!");
                        CompletableFuture<List<TaskHolder>> pending = stopped.getAndSet(
                                CompletableFuture.completedFuture(List.of()));
                        List<TaskHolder> tasks = clearAll();
                        if (pending != null) pending.complete(tasks);
                    }
                    timerThread = thread;
                } else {
                    completeStop(stop);
                }
                throw error;
            }
        }

        private void nextTick() {
            for (;;) {
                long time = clock();
                long sleepTime = start + totalTick * tickNanos - time;
                if (sleepTime > 0) {
                    checkQueue(time);
                    waitNanos(sleepTime);
                } else {
                    int bucket = tick & wheelMask;
                    TaskQueue tasks = wheel[bucket];
                    TaskQueue putBack = spareTaskQueue;
                    AbstractNodeQueue.Node<TaskHolder> node;
                    while ((node = tasks.pollNode()) != null) {
                        TaskHolder task = node.value;
                        if (!task.isCancelled()) {
                            if (task.ticks >= wheelSize) {
                                task.ticks -= wheelSize;
                                putBack.addNode(node);
                            } else {
                                task.executeTask();
                            }
                        }
                    }
                    wheel[bucket] = putBack;
                    spareTaskQueue = tasks;
                    tick++;
                    totalTick++;
                }
                CompletableFuture<List<TaskHolder>> stop = stopped.get();
                if (stop != null) {
                    completeStop(stop);
                    return;
                }
            }
        }

        private void completeStop(CompletableFuture<List<TaskHolder>> stop) {
            boolean replaced = stopped.compareAndSet(stop, CompletableFuture.completedFuture(List.of()));
            assert replaced : "Stop signal violated in LARS";
            stop.complete(clearAll());
        }
    }

    private static final class TaskQueue extends AbstractNodeQueue<TaskHolder> {
    }

    private final class TaskHolder implements Runnable, Cancellable {
        private static final VarHandle taskHandle;

        static {
            try {
                taskHandle = MethodHandles.lookup().findVarHandle(TaskHolder.class, "task", Runnable.class);
            } catch (ReflectiveOperationException error) {
                throw new ExceptionInInitializerError(error);
            }
        }

        private volatile Runnable task;
        private int ticks;
        private final Executor executionContext;

        private TaskHolder(Runnable task, int ticks, Executor executionContext) {
            this.task = task;
            this.ticks = ticks;
            this.executionContext = executionContext;
        }

        private Runnable extractTask(Runnable replaceWith) {
            for (;;) {
                Runnable current = task;
                if (current == ExecutedTask || current == CancelledTask) return current;
                if (taskHandle.compareAndSet(this, current, replaceWith)) return current;
            }
        }

        private boolean executeTask() {
            Runnable current = extractTask(ExecutedTask);
            if (current == ExecutedTask || current == CancelledTask) return false;
            try {
                executionContext.execute(current);
                return true;
            } catch (Throwable error) {
                if (error instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                ActorCell.rethrowIfFatal(error);
                reportFailure(executionContext, error);
                return false;
            }
        }

        @Override
        public void run() {
            extractTask(ExecutedTask).run();
        }

        @Override
        public boolean cancel() {
            Runnable current = extractTask(CancelledTask);
            if (current == ExecutedTask || current == CancelledTask) return false;
            if (current instanceof SchedulerTask schedulerTask) {
                try {
                    executionContext.execute(schedulerTask::cancelled);
                } catch (Throwable error) {
                    ActorCell.rethrowIfFatal(error);
                    reportFailure(executionContext, error);
                }
            }
            return true;
        }

        @Override
        public boolean isCancelled() {
            return task == CancelledTask;
        }
    }

    private static void reportFailure(Executor executor, Throwable error) {
        if (executor instanceof ExecutionContext context) {
            context.reportFailure(error);
        } else {
            Thread thread = Thread.currentThread();
            thread.getUncaughtExceptionHandler().uncaughtException(thread, error);
        }
    }

    private static final class SchedulerException extends RuntimeException {
        private SchedulerException(String message) {
            super(message, null, false, false);
        }
    }
}
