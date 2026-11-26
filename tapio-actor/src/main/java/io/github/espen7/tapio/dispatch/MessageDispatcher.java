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

import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEventException;

import java.time.Duration;

/**
 * 所有 MessageDispatcher 共用的 Actor 注册、inhabitants 和延迟 shutdown 生命周期。
 */
public abstract class MessageDispatcher extends AbstractMessageDispatcher implements ExecutionContext, AutoCloseable {
    public static final int UNSCHEDULED = 0;
    public static final int SCHEDULED = 1;
    public static final int RESCHEDULED = 2;

    protected final MessageDispatcherConfigurator configurator;
    protected final Mailboxes mailboxes;
    protected final EventStream eventStream;
    private final Scheduler scheduler;

    @SuppressWarnings("unused")
    private volatile long _inhabitantsDoNotCallMeDirectly;
    @SuppressWarnings("unused")
    private volatile int _shutdownScheduleDoNotCallMeDirectly;

    private final Runnable taskCleanup = () -> {
        if (addInhabitants(-1) == 0) ifSensibleToDoSoThenScheduleShutdown();
    };

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
     * 从 configurator 的 prerequisites 创建通用 Dispatcher 状态。
     *
     * @param configurator 创建此 Dispatcher 的 configurator
     */
    protected MessageDispatcher(MessageDispatcherConfigurator configurator) {
        this.configurator = configurator;
        this.mailboxes = configurator.prerequisites().mailboxes();
        this.eventStream = configurator.prerequisites().eventStream();
        this.scheduler = configurator.prerequisites().scheduler();
    }

    /**
     * 返回创建此 Dispatcher 的 {@link MessageDispatcherConfigurator}。
     *
     * @return 对应的 configurator
     */
    public MessageDispatcherConfigurator configurator() {
        return configurator;
    }

    /**
     * 返回当前实现用作 dead letters sink 的消息队列。
     *
     * @return dead letters message queue
     */
    public final MessageQueue deadLetters() {
        return mailboxes.deadLetterMailbox().messageQueue();
    }

    /**
     * 返回当前注册 Actor 与未完成普通任务的总数。
     *
     * @return inhabitants
     */
    public final long inhabitants() {
        return (long) inhabitantsHandle.getVolatile(this);
    }

    private final long addInhabitants(long add) {
        long old = (long) inhabitantsHandle.getAndAdd(this, add);
        long ret = old + add;
        if (ret < 0) {
            IllegalStateException e = new IllegalStateException(
                    "ACTOR SYSTEM CORRUPTED!!! A dispatcher can't have less than 0 inhabitants!");
            reportFailure(e);
            throw e;
        }
        return ret;
    }

    private final int shutdownSchedule() {
        return (int) shutdownScheduleHandle.getVolatile(this);
    }

    private final boolean updateShutdownSchedule(int expect, int update) {
        return shutdownScheduleHandle.compareAndSet(this, expect, update);
    }

    /**
     * 为 ActorCell 创建 Mailbox。
     *
     * @param actor ActorCell
     * @param mailboxType 消息队列类型
     * @return Mailbox
     */
    public abstract Mailbox createMailbox(ActorCell actor, MailboxType mailboxType);

    /**
     * 返回 Dispatcher 的完整配置 id。
     *
     * @return 配置 id
     */
    public abstract String id();

    /**
     * 注册 ActorCell 并首次调度其 mailbox。
     *
     * @param actor ActorCell
     */
    public final void attach(ActorCell actor) {
        register(actor);
        registerForExecution(actor.mailbox(), false, true);
    }

    /**
     * 注销 ActorCell，并在没有 inhabitants 时安排延迟 shutdown。
     *
     * @param actor ActorCell
     */
    public final void detach(ActorCell actor) {
        try {
            unregister(actor);
        } finally {
            ifSensibleToDoSoThenScheduleShutdown();
        }
    }

    protected final boolean resubmitOnBlock() {
        return true;
    }

    @Override
    protected final void unbatchedExecute(Runnable r) {
        TaskInvocation invocation = new TaskInvocation(eventStream, r, taskCleanup);
        addInhabitants(+1);
        try {
            executeTask(invocation);
        } catch (Throwable t) {
            addInhabitants(-1);
            throw t;
        }
    }

    /**
     * 报告执行或提交任务时发生的非致命异常。
     *
     * @param t 异常
     */
    @Override
    public void reportFailure(Throwable t) {
        if (t instanceof LogEventException e) {
            DispatchFailureReporting.report(eventStream, e.event());
        } else {
            DispatchFailureReporting.report(eventStream,
                    new Error(t, getClass().getName(), getClass(), t.getMessage()));
        }
    }

    private final void ifSensibleToDoSoThenScheduleShutdown() {
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
            scheduler.scheduleOnce(shutdownTimeout(), shutdownAction, new ExecutionContext() {
                @Override
                public void execute(Runnable runnable) {
                    runnable.run();
                }

                @Override
                public void reportFailure(Throwable t) {
                    MessageDispatcher.this.reportFailure(t);
                }
            });
        } catch (IllegalStateException schedulerClosed) {
            shutdown();
            updateShutdownSchedule(SCHEDULED, UNSCHEDULED);
        }
    }

    /**
     * 增加一个已注册 Actor 的 inhabitants。
     *
     * @param actor ActorCell
     */
    public void register(ActorCell actor) {
        addInhabitants(+1);
    }

    /**
     * 释放 Actor 的 inhabitants，并把 mailbox 切到 dead letters。
     *
     * @param actor ActorCell
     */
    public void unregister(ActorCell actor) {
        addInhabitants(-1);
        Mailbox mailBox = actor.swapMailbox(mailboxes.deadLetterMailbox());
        mailBox.becomeClosed();
        mailBox.cleanUp();
    }

    /**
     * 挂起指定 Actor 的新消息处理。
     *
     * @param actor ActorCell
     */
    public void suspend(ActorCell actor) {
        Mailbox mbox = actor.mailbox();
        if ((mbox.actor() == actor) && (mbox.dispatcher() == this)) mbox.suspend();
    }

    /**
     * 恢复指定 Actor 的消息处理。
     *
     * @param actor ActorCell
     */
    public void resume(ActorCell actor) {
        Mailbox mbox = actor.mailbox();
        if ((mbox.actor() == actor) && (mbox.dispatcher() == this) && mbox.resume()) {
            registerForExecution(mbox, false, false);
        }
    }

    /**
     * 将普通消息放入目标 mailbox 并调度执行。
     *
     * @param receiver ActorCell
     * @param invocation 用户消息
     */
    public abstract void dispatch(ActorCell receiver, Envelope invocation);

    /**
     * 将系统消息放入目标 mailbox 并调度执行。
     *
     * @param receiver ActorCell
     * @param invocation 系统消息
     */
    public abstract void systemDispatch(ActorCell receiver, SystemMessage invocation);

    /**
     * 尝试把 mailbox 标记为 Scheduled 并提交执行。
     *
     * @param mbox mailbox
     * @param hasMessageHint 普通消息提示
     * @param hasSystemMessageHint 系统消息提示
     * @return 成功提交时返回 {@code true}
     */
    public abstract boolean registerForExecution(Mailbox mbox, boolean hasMessageHint, boolean hasSystemMessageHint);

    /**
     * 返回 mailbox 连续处理普通消息的上限。
     *
     * @return throughput
     */
    public abstract int throughput();

    /**
     * 返回 mailbox 批次的截止时间。
     *
     * @return 截止时间
     */
    public abstract Duration throughputDeadlineTime();

    /**
     * 判断是否配置了批次截止时间。
     *
     * @return 截止时间大于零时返回 {@code true}
     */
    public final boolean isThroughputDeadlineTimeDefined() {
        return throughputDeadlineTime().toMillis() > 0;
    }

    /**
     * 将 TaskInvocation 提交给实际 executor。
     *
     * @param invocation 任务
     */
    public abstract void executeTask(TaskInvocation invocation);

    /**
     * 返回没有 Actor 注册后等待 shutdown 的时间。
     *
     * @return shutdown 超时
     */
    public abstract Duration shutdownTimeout();

    /**
     * 关闭当前 executor；后续任务会延迟创建新的 executor。
     */
    public abstract void shutdown();

    /**
     * 关闭当前 executor。
     */
    @Override
    public final void close() {
        shutdown();
    }
}
