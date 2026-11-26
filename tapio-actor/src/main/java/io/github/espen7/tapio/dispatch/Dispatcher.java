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
package io.github.espen7.tapio.dispatch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.EventStream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

/**
 * Dispatcher 将 Envelope 或 SystemMessage 放入目标 Actor 的 Mailbox，并把 Mailbox
 * 提交给 Executor。它不直接调用 Actor，Actor 的串行执行由 Mailbox 的 Scheduled
 * 状态保证。
 */
public class Dispatcher implements ExecutionContext, AutoCloseable {
    private static final int UNSCHEDULED = 0;
    private static final int SCHEDULED = 1;
    private static final int RESCHEDULED = 2;

    private static final VarHandle INHABITANTS_HANDLE;
    private static final VarHandle SHUTDOWN_SCHEDULE_HANDLE;
    private static final VarHandle EXECUTOR_SERVICE_DELEGATE_HANDLE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            INHABITANTS_HANDLE = lookup.findVarHandle(Dispatcher.class,
                    "_inhabitantsDoNotCallMeDirectly", long.class);
            SHUTDOWN_SCHEDULE_HANDLE = lookup.findVarHandle(Dispatcher.class,
                    "_shutdownScheduleDoNotCallMeDirectly", int.class);
            EXECUTOR_SERVICE_DELEGATE_HANDLE = lookup.findVarHandle(Dispatcher.class,
                    "executorServiceDelegate", LazyExecutorServiceDelegate.class);
        } catch (ReflectiveOperationException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    @SuppressWarnings("unused")
    private volatile long _inhabitantsDoNotCallMeDirectly;
    @SuppressWarnings("unused")
    private volatile int _shutdownScheduleDoNotCallMeDirectly;
    @SuppressWarnings("unused")
    private volatile LazyExecutorServiceDelegate executorServiceDelegate;

    private final EventStream eventStream;
    private final Scheduler scheduler;
    private final Duration shutdownTimeout;
    private final DeadLetterMailbox deadLetterMailbox;
    private final int throughput;
    private final Duration throughputDeadlineTime;

    private final Scheduler.TaskRunOnClose shutdownAction = new Scheduler.TaskRunOnClose() {
        @Override
        public void run() {
            for (; ; ) {
                int schedule = shutdownSchedule();
                switch (schedule) {
                    case SCHEDULED -> {
                        try {
                            if (inhabitants() == 0) shutdown();
                        } finally {
                            while (!updateShutdownSchedule(shutdownSchedule(), UNSCHEDULED)) {
                                Thread.onSpinWait();
                            }
                        }
                        return;
                    }
                    case RESCHEDULED -> {
                        if (updateShutdownSchedule(RESCHEDULED, SCHEDULED)) {
                            scheduleShutdownAction();
                            return;
                        }
                    }
                    case UNSCHEDULED -> {
                        return;
                    }
                    default -> throw new IllegalArgumentException(
                            "Unexpected shutdown schedule marker: " + schedule);
                }
            }
        }
    };

    /**
     * 创建使用 ActorSystem 有效配置中默认 Dispatcher 参数的 Dispatcher。
     *
     * @param system 所属 ActorSystem
     */
    public Dispatcher(ActorSystem system) {
        this(defaultDispatcherConfig(system).getInt("throughput"), system);
    }

    /**
     * 创建使用指定 mailbox throughput 的 Dispatcher。
     *
     * @param throughput 每次 mailbox task 最多连续处理的普通消息数
     * @param system 所属 ActorSystem
     */
    public Dispatcher(int throughput, ActorSystem system) {
        this(throughput, Duration.ZERO, system);
    }

    Dispatcher(int throughput, Duration throughputDeadlineTime, ActorSystem system) {
        this(throughput, throughputDeadlineTime, system, system.scheduler(),
                defaultDispatcherConfig(system).getDuration("shutdown-timeout"),
                defaultExecutorServiceFactory(system));
    }

    Dispatcher(int throughput, Duration throughputDeadlineTime, ActorSystem system, Scheduler scheduler,
               Duration shutdownTimeout, ExecutorServiceFactory executorServiceFactory) {
        this.eventStream = system.eventStream();
        this.scheduler = scheduler;
        this.shutdownTimeout = shutdownTimeout;
        this.throughput = throughput;
        this.throughputDeadlineTime = throughputDeadlineTime;
        this.executorServiceDelegate = new LazyExecutorServiceDelegate(executorServiceFactory);
        this.deadLetterMailbox = new DeadLetterMailbox(system);
    }

    static ExecutorServiceFactory defaultExecutorServiceFactory(ActorSystem system) {
        return DispatcherConfigurator.createExecutorServiceFactory(defaultDispatcherConfig(system), system.threadFactory());
    }

    private static Config defaultDispatcherConfig(ActorSystem system) {
        return ConfigFactory.parseMap(Map.of("id", Dispatchers.DefaultDispatcherId))
                .withFallback(system.settings().config().getConfig(Dispatchers.DefaultDispatcherId));
    }

    /**
     * 返回每个 mailbox task 连续处理普通消息的上限。
     *
     * @return throughput 上限
     */
    public int throughput() {
        return throughput;
    }

    Duration throughputDeadlineTime() {
        return throughputDeadlineTime;
    }

    boolean isThroughputDeadlineTimeDefined() {
        return throughputDeadlineTime.toMillis() > 0;
    }

    /**
     * 返回当前实现用作 dead letters sink 的消息队列。
     *
     * @return dead letters message queue
     */
    public MessageQueue deadLetters() {
        return deadLetterMailbox.messageQueue();
    }

    /**
     * 为 ActorCell 创建 Mailbox，对应上游 {@code Dispatcher.createMailbox}。
     *
     * @param actor 即将拥有此 Mailbox 的 ActorCell
     * @param mailboxType 创建普通消息队列的 mailbox type
     * @return 绑定新消息队列的 Mailbox
     */
    public Mailbox createMailbox(ActorCell actor, MailboxType mailboxType) {
        return new Mailbox(mailboxType.create(actor.self(), actor.system()));
    }

    Mailbox deadLetterMailbox() {
        return deadLetterMailbox;
    }

    /**
     * INTERNAL API：注册 ActorCell 并首次调度其 mailbox。
     *
     * @param actor 已初始化并放入 Create 系统消息的 ActorCell
     */
    @InternalApi
    public final void attach(ActorCell actor) {
        register(actor);
        registerForExecution(actor.mailbox(), false, true);
    }

    /**
     * INTERNAL API：注销 ActorCell，并在没有 inhabitants 时安排延迟 shutdown。
     *
     * @param actor 待注销的 ActorCell
     */
    @InternalApi
    public final void detach(ActorCell actor) {
        try {
            unregister(actor);
        } finally {
            ifSensibleToDoSoThenScheduleShutdown();
        }
    }

    /**
     * INTERNAL API：增加一个已注册 Actor 的 inhabitants 计数。
     *
     * @param actor 待注册的 ActorCell
     */
    @InternalApi
    public void register(ActorCell actor) {
        addInhabitants(1);
    }

    /**
     * INTERNAL API：释放 Actor 的 inhabitants，并将其 mailbox 切到 dead letters。
     *
     * @param actor 待注销的 ActorCell
     */
    @InternalApi
    public void unregister(ActorCell actor) {
        addInhabitants(-1);
        Mailbox mailbox = actor.swapMailbox(deadLetterMailbox);
        mailbox.close();
        mailbox.cleanUp();
    }

    /**
     * 普通消息入口，对应 Pekko Dispatcher.dispatch。
     *
     * @param receiver 接收消息的 ActorCell
     * @param invocation 包含用户消息和 sender 的 Envelope
     */
    public void dispatch(ActorCell receiver, Envelope invocation) {
        Mailbox mailbox = receiver.mailbox();
        mailbox.enqueue(receiver.self(), invocation);
        registerForExecution(mailbox, true, false);
    }

    /**
     * 系统消息入口，对应 Pekko Dispatcher.systemDispatch。
     *
     * @param receiver 接收系统消息的 ActorCell
     * @param invocation 生命周期或 runtime 控制消息
     */
    public void systemDispatch(ActorCell receiver, SystemMessage invocation) {
        Mailbox mailbox = receiver.mailbox();
        mailbox.systemEnqueue(receiver.self(), invocation);
        registerForExecution(mailbox, false, true);
    }

    /**
     * 尝试把 Mailbox 变成 Scheduled 并提交给 Executor。
     * has*Hint 是发送方已知的快速路径，真正的队列状态仍由 Mailbox 再次检查。
     *
     * @param mailbox 待调度的 Mailbox
     * @param hasMessageHint 调用方是否确认普通消息队列非空
     * @param hasSystemMessageHint 调用方是否确认系统消息队列非空
     * @return 本次调用成功提交 task 时返回 {@code true}
     * @throws java.util.concurrent.RejectedExecutionException Executor 两次拒绝提交时
     */
    public boolean registerForExecution(Mailbox mailbox, boolean hasMessageHint, boolean hasSystemMessageHint) {
        if (!mailbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) return false;
        if (!mailbox.setAsScheduled()) return false;
        try {
            executorService().execute(mailbox);
            return true;
        } catch (RejectedExecutionException firstFailure) {
            try {
                executorService().execute(mailbox);
                return true;
            } catch (RejectedExecutionException secondFailure) {
                mailbox.setAsIdle();
                DispatchFailureReporting.report(eventStream,
                        new Error(getClass().getName(), getClass(),
                                "registerForExecution was rejected twice!", firstFailure));
                throw secondFailure;
            }
        }
    }

    @Override
    public void execute(Runnable runnable) {
        unbatchedExecute(runnable);
    }

    private void unbatchedExecute(Runnable runnable) {
        TaskInvocation invocation = new TaskInvocation(eventStream, runnable, this::taskCleanup);
        addInhabitants(1);
        try {
            executeTask(invocation);
        } catch (Throwable error) {
            addInhabitants(-1);
            throw error;
        }
    }

    void executeTask(TaskInvocation invocation) {
        try {
            executorService().execute(invocation);
        } catch (RejectedExecutionException firstFailure) {
            try {
                executorService().execute(invocation);
            } catch (RejectedExecutionException secondFailure) {
                DispatchFailureReporting.report(eventStream,
                        new Error(getClass().getName(), getClass(),
                                "executeTask was rejected twice!", firstFailure));
                throw secondFailure;
            }
        }
    }

    @Override
    public void reportFailure(Throwable cause) {
        DispatchFailureReporting.report(eventStream,
                new Error(getClass().getName(), getClass(), String.valueOf(cause.getMessage()), cause));
    }

    long inhabitants() {
        return (long) INHABITANTS_HANDLE.getVolatile(this);
    }

    private long addInhabitants(long add) {
        long old = (long) INHABITANTS_HANDLE.getAndAdd(this, add);
        long result = old + add;
        if (result < 0) {
            IllegalStateException error = new IllegalStateException(
                    "ACTOR SYSTEM CORRUPTED!!! A dispatcher can't have less than 0 inhabitants!");
            reportFailure(error);
            throw error;
        }
        return result;
    }

    private int shutdownSchedule() {
        return (int) SHUTDOWN_SCHEDULE_HANDLE.getVolatile(this);
    }

    private boolean updateShutdownSchedule(int expect, int update) {
        return SHUTDOWN_SCHEDULE_HANDLE.compareAndSet(this, expect, update);
    }

    private void ifSensibleToDoSoThenScheduleShutdown() {
        while (inhabitants() <= 0) {
            int schedule = shutdownSchedule();
            switch (schedule) {
                case UNSCHEDULED -> {
                    if (updateShutdownSchedule(UNSCHEDULED, SCHEDULED)) {
                        scheduleShutdownAction();
                        return;
                    }
                }
                case SCHEDULED -> {
                    if (updateShutdownSchedule(SCHEDULED, RESCHEDULED)) return;
                }
                case RESCHEDULED -> {
                    return;
                }
                default -> throw new IllegalArgumentException(
                        "Unexpected shutdown schedule marker: " + schedule);
            }
            Thread.onSpinWait();
        }
    }

    private void scheduleShutdownAction() {
        try {
            scheduler.scheduleOnce(shutdownTimeout, shutdownAction);
        } catch (IllegalStateException schedulerClosed) {
            shutdown();
            updateShutdownSchedule(SCHEDULED, UNSCHEDULED);
        }
    }

    private void taskCleanup() {
        if (addInhabitants(-1) == 0) ifSensibleToDoSoThenScheduleShutdown();
    }

    private ExecutorService executorService() {
        return executorServiceDelegate.executor();
    }

    void shutdown() {
        LazyExecutorServiceDelegate newDelegate = executorServiceDelegate.copy();
        LazyExecutorServiceDelegate oldDelegate = (LazyExecutorServiceDelegate)
                EXECUTOR_SERVICE_DELEGATE_HANDLE.getAndSet(this, newDelegate);
        oldDelegate.shutdown();
    }

    /** 关闭当前 executor delegate；后续任务会延迟创建新的 delegate。 */
    @Override
    public void close() {
        shutdown();
    }

    private static final class LazyExecutorServiceDelegate {
        private final ExecutorServiceFactory factory;
        @SuppressWarnings("unused")
        private volatile ExecutorService executor;

        private LazyExecutorServiceDelegate(ExecutorServiceFactory factory) {
            this.factory = factory;
        }

        private ExecutorService executor() {
            ExecutorService current = executor;
            if (current == null) {
                synchronized (this) {
                    current = executor;
                    if (current == null) {
                        current = factory.createExecutorService();
                        executor = current;
                    }
                }
            }
            return current;
        }

        private LazyExecutorServiceDelegate copy() {
            return new LazyExecutorServiceDelegate(factory);
        }

        private void shutdown() {
            executor().shutdown();
        }
    }
}
