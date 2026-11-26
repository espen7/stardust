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
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.RejectedExecutionException;

/**
 * 将 Envelope 或 SystemMessage 放入目标 Actor 的 Mailbox，并把 Mailbox 提交给 Executor。
 * 它不直接调用 Actor，Actor 的串行执行由 Mailbox 的 Scheduled 状态保证。
 */
public class Dispatcher extends MessageDispatcher {
    private final String id;
    private final int throughput;
    private final Duration throughputDeadlineTime;
    private final Duration shutdownTimeout;
    private final boolean isVirtualized;

    @SuppressWarnings("unused")
    private volatile LazyExecutorServiceDelegate executorServiceDelegate;

    private final AtomicReferenceFieldUpdater<Dispatcher, LazyExecutorServiceDelegate> esUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Dispatcher.class, LazyExecutorServiceDelegate.class,
                    "executorServiceDelegate");

    private class LazyExecutorServiceDelegate implements ExecutorServiceDelegate {
        private final ExecutorServiceFactory factory;
        @SuppressWarnings("unused")
        private volatile ExecutorService executor;

        private LazyExecutorServiceDelegate(ExecutorServiceFactory factory) {
            this.factory = factory;
        }

        private LazyExecutorServiceDelegate copy() {
            return new LazyExecutorServiceDelegate(factory);
        }

        @Override
        public ExecutorService executor() {
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

        @Override
        public void shutdown() {
            executor().shutdown();
        }
    }

    /**
     * 创建使用完整配置 id 的普通 Dispatcher。
     *
     * @param _configurator 创建此 Dispatcher 的 configurator
     * @param id 完整配置 id
     * @param throughput 每次 mailbox task 最多连续处理的普通消息数
     * @param throughputDeadlineTime mailbox 批次的截止时间
     * @param executorServiceFactoryProvider executor 工厂提供者
     * @param shutdownTimeout 没有 Actor 注册后等待 shutdown 的时间
     */
    public Dispatcher(MessageDispatcherConfigurator _configurator, String id, int throughput,
                      Duration throughputDeadlineTime, ExecutorServiceFactoryProvider executorServiceFactoryProvider,
                      Duration shutdownTimeout) {
        super(_configurator);
        this.id = id;
        this.throughput = throughput;
        this.throughputDeadlineTime = throughputDeadlineTime;
        this.shutdownTimeout = shutdownTimeout;
        this.isVirtualized = executorServiceFactoryProvider.isVirtualized();
        this.executorServiceDelegate = new LazyExecutorServiceDelegate(
                executorServiceFactoryProvider.createExecutorServiceFactory(id, configurator.prerequisites().threadFactory()));
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public int throughput() {
        return throughput;
    }

    @Override
    public Duration throughputDeadlineTime() {
        return throughputDeadlineTime;
    }

    @Override
    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    protected final ExecutorServiceDelegate executorService() {
        return executorServiceDelegate;
    }

    @Override
    public Mailbox createMailbox(ActorCell actor, MailboxType mailboxType) {
        return new Mailbox(mailboxType.create(actor.self(), actor.system()));
    }

    @Override
    public final boolean batchable(Runnable runnable) {
        if (isVirtualized) return false;
        return super.batchable(runnable);
    }

    @Override
    public void dispatch(ActorCell receiver, Envelope invocation) {
        Mailbox mbox = receiver.mailbox();
        mbox.enqueue(receiver.self(), invocation);
        registerForExecution(mbox, true, false);
    }

    @Override
    public void systemDispatch(ActorCell receiver, SystemMessage invocation) {
        Mailbox mbox = receiver.mailbox();
        mbox.systemEnqueue(receiver.self(), invocation);
        registerForExecution(mbox, false, true);
    }

    @Override
    public boolean registerForExecution(Mailbox mbox, boolean hasMessageHint, boolean hasSystemMessageHint) {
        if (!mbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) return false;
        if (!mbox.setAsScheduled()) return false;
        try {
            executorService().execute(mbox);
            return true;
        } catch (RejectedExecutionException e) {
            try {
                executorService().execute(mbox);
                return true;
            } catch (RejectedExecutionException e2) {
                mbox.setAsIdle();
                DispatchFailureReporting.report(eventStream,
                        new Error(getClass().getName(), getClass(),
                                "registerForExecution was rejected twice!", e));
                throw e2;
            }
        }
    }

    @Override
    public void executeTask(TaskInvocation invocation) {
        try {
            executorService().execute(invocation);
        } catch (RejectedExecutionException e) {
            try {
                executorService().execute(invocation);
            } catch (RejectedExecutionException e2) {
                DispatchFailureReporting.report(eventStream,
                        new Error(getClass().getName(), getClass(),
                                "executeTask was rejected twice!", e));
                throw e2;
            }
        }
    }

    @Override
    public void shutdown() {
        LazyExecutorServiceDelegate newDelegate = executorServiceDelegate.copy();
        LazyExecutorServiceDelegate es = (LazyExecutorServiceDelegate) esUpdater.getAndSet(this, newDelegate);
        es.shutdown();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + id + "]";
    }
}
