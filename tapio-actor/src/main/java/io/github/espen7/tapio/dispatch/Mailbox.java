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
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.dispatch.sysmsg.NoMessage;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.util.NonFatal;

/**
 * Actor 的双队列 Mailbox。
 *
 * <p>普通消息保存在 MessageQueue，系统消息直接链接在 SystemMessage 对象上。
 * 系统消息队列使用头插，因此入队是 O(1)，执行前通过反转恢复 FIFO 顺序。
 * status 的位布局沿用 Pekko，Scheduled 位用于保证同一个 Mailbox 同时只有一个
 * Dispatcher task。</p>
 */
public abstract class Mailbox implements SystemMessageQueue, Runnable {
    public static final int Open = 0;
    public static final int Closed = 1;
    public static final int Scheduled = 2;
    public static final int shouldScheduleMask = 3;
    public static final int shouldNotProcessMask = ~2;
    public static final int suspendMask = ~3;
    public static final int suspendUnit = 4;

    private final MessageQueue messageQueue;
    @SuppressWarnings("unused")
    private volatile int _statusDoNotCallMeDirectly;
    @SuppressWarnings("unused")
    private volatile SystemMessage _systemQueueDoNotCallMeDirectly;
    private volatile ActorCell actor;

    /**
     * 创建一个 Mailbox，并绑定普通消息队列。
     *
     * @param messageQueue 普通用户消息队列
     */
    public Mailbox(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    /**
     * 将 Mailbox 绑定到所属 ActorCell 和调度它的 Dispatcher。
     */
    public void setActor(ActorCell cell) {
        actor = cell;
    }

    /**
     * @return 此 Mailbox 所属的 ActorCell
     */
    public ActorCell actor() {
        return actor;
    }

    /**
     * 返回调度此 Mailbox 的 MessageDispatcher。
     *
     * @return 调度此 Mailbox 的 MessageDispatcher
     */
    public MessageDispatcher dispatcher() {
        return actor.dispatcher();
    }

    /**
     * @return 此 Mailbox 的普通消息队列
     */
    public MessageQueue messageQueue() {
        return messageQueue;
    }

    /**
     * 将 Envelope 放入普通消息队列。
     */
    public void enqueue(ActorRef receiver, Envelope message) {
        messageQueue.enqueue(receiver, message);
    }

    /**
     * 从普通消息队列取出下一条 Envelope；队列为空时返回 {@code null}。
     */
    public Envelope dequeue() {
        return messageQueue.dequeue();
    }

    public boolean hasMessages() {
        return messageQueue.hasMessages();
    }

    public int numberOfMessages() {
        return messageQueue.numberOfMessages();
    }

    final int currentStatus() {
        return (int) AbstractMailbox.mailboxStatusHandle.getVolatile(this);
    }

    protected final boolean updateStatus(int oldStatus, int newStatus) {
        return AbstractMailbox.mailboxStatusHandle.compareAndSet(this, oldStatus, newStatus);
    }

    protected final void setStatus(int newStatus) {
        AbstractMailbox.mailboxStatusHandle.setVolatile(this, newStatus);
    }

    /**
     * @return Mailbox 未关闭且 suspend count 为零时返回 {@code true}
     */
    public final boolean shouldProcessMessage() {
        return (currentStatus() & shouldNotProcessMask) == 0;
    }

    /**
     * @return 当前嵌套 suspend 次数
     */
    public final int suspendCount() {
        return currentStatus() / suspendUnit;
    }

    /**
     * @return suspend count 大于零时返回 {@code true}
     */
    public final boolean isSuspended() {
        return (currentStatus() & suspendMask) != 0;
    }

    /**
     * @return Mailbox 已关闭时返回 {@code true}
     */
    public final boolean isClosed() {
        return currentStatus() == Closed;
    }

    /**
     * @return Mailbox task 已提交或正在执行时返回 {@code true}
     */
    public final boolean isScheduled() {
        return (currentStatus() & Scheduled) != 0;
    }

    /**
     * 将 suspend count 减一。
     *
     * @return 操作后 suspend count 为零时返回 {@code true}
     */
    public final boolean resume() {
        for (; ; ) {
            int old = currentStatus();
            if (old == Closed) {
                setStatus(Closed);
                return false;
            }
            int next = old < suspendUnit ? old : old - suspendUnit;
            if (updateStatus(old, next)) return next < suspendUnit;
            Thread.onSpinWait();
        }
    }

    /**
     * 将 suspend count 加一。
     *
     * @return 操作前 suspend count 为零时返回 {@code true}
     */
    public final boolean suspend() {
        for (; ; ) {
            int old = currentStatus();
            if (old == Closed) {
                setStatus(Closed);
                return false;
            }
            if (updateStatus(old, old + suspendUnit)) return old < suspendUnit;
            Thread.onSpinWait();
        }
    }

    /**
     * 原子地将 Mailbox 状态设为 Closed。
     *
     * @return 本次调用首次关闭 Mailbox 时返回 {@code true}
     */
    public final boolean becomeClosed() {
        for (; ; ) {
            int old = currentStatus();
            if (old == Closed) {
                setStatus(Closed);
                return false;
            }
            if (updateStatus(old, Closed)) return true;
            Thread.onSpinWait();
        }
    }

    /**
     * CAS 设置 Scheduled，失败表示已经有 task 在运行或等待运行。
     *
     * @return 本次调用成功设置 Scheduled 时返回 {@code true}
     */
    public final boolean setAsScheduled() {
        for (; ; ) {
            int old = currentStatus();
            if ((old & shouldScheduleMask) != Open) return false;
            if (updateStatus(old, old | Scheduled)) return true;
            Thread.onSpinWait();
        }
    }

    /**
     * 清除 Scheduled，并返回这次是否真的改变了状态。
     *
     * @return 状态更新成功时返回 {@code true}
     */
    public final boolean setAsIdle() {
        for (; ; ) {
            int old = currentStatus();
            if (updateStatus(old, old & ~Scheduled)) return true;
            Thread.onSpinWait();
        }
    }

    protected final SystemMessage systemQueueGet() {
        return (SystemMessage) AbstractMailbox.systemMessageHandle.getVolatile(this);
    }

    protected final boolean systemQueuePut(SystemMessage oldContents, SystemMessage newContents) {
        return oldContents == newContents
                || AbstractMailbox.systemMessageHandle.compareAndSet(this, oldContents, newContents);
    }

    /**
     * 根据状态位和队列提示判断 Mailbox 是否可以提交给 Executor。
     *
     * @param hasMessageHint       普通消息队列非空提示
     * @param hasSystemMessageHint 系统消息队列非空提示
     * @return 当前可调度时返回 {@code true}
     */
    public final boolean canBeScheduledForExecution(boolean hasMessageHint, boolean hasSystemMessageHint) {
        int current = currentStatus();
        if (current == Open || current == Scheduled) {
            return hasMessageHint || hasSystemMessageHint || hasSystemMessages() || hasMessages();
        }
        if (current == Closed) return false;
        return hasSystemMessageHint || hasSystemMessages();
    }

    /**
     * Executor 执行入口。顺序必须是 system messages、user messages、重新注册。
     * finally 中重新注册可以覆盖执行期间新到达的消息。
     */
    @Override
    public final void run() {
        try {
            if (!isClosed()) {
                processAllSystemMessages();
                processMailbox();
            }
        } finally {
            setAsIdle();
            dispatcher().registerForExecution(this, false, false);
        }
    }

    /**
     * 按 Dispatcher throughput 批量处理普通消息。
     */
    private void processMailbox() {
        int left = Math.max(dispatcher().throughput(), 1);
        boolean deadlineDefined = dispatcher().isThroughputDeadlineTimeDefined();
        long deadlineNs = deadlineDefined ? System.nanoTime() + dispatcher().throughputDeadlineTime().toNanos() : 0L;
        while (left-- > 0 && shouldProcessMessage()) {
            Envelope next = dequeue();
            if (next == null) break;
            actor.invoke(next);
            if (Thread.interrupted()) {
                ActorCell.<RuntimeException>throwUnchecked(
                        new InterruptedException("Interrupted while processing actor messages"));
            }
            processAllSystemMessages();
            if (deadlineDefined && System.nanoTime() - deadlineNs >= 0) break;
        }
    }


    /**
     * 摘取系统消息队列并按入队顺序调用 ActorCell.systemInvoke。
     */
    public final void processAllSystemMessages() {
        InterruptedException interruption = null;
        SystemMessage messageList = systemDrain(null);
        while (messageList != null && !isClosed()) {
            SystemMessage message = messageList;
            messageList = message.next();
            message.unlink();
            actor.systemInvoke(message);
            if (Thread.interrupted()) {
                interruption = new InterruptedException("Interrupted while processing system messages");
            }
            if (messageList == null && !isClosed()) messageList = systemDrain(null);
        }

        Mailbox dlm = messageList != null ? dispatcher().mailboxes.deadLetterMailbox() : null;
        while (messageList != null) {
            SystemMessage message = messageList;
            messageList = message.next();
            message.unlink();
            try {
                dlm.systemEnqueue(actor.self(), message);
            } catch (Throwable error) {
                if (error instanceof InterruptedException interrupted) {
                    interruption = interrupted;
                } else if (NonFatal.apply(error)) {
                    actor.system().eventStream().publish(new Error(actor.self().path().toString(), getClass(),
                            "error while enqueuing " + message + " to deadLetters: " + error.getMessage(), error));
                } else {
                    ActorCell.<RuntimeException>throwUnchecked(error);
                }
            }
        }
        if (interruption != null) {
            Thread.interrupted();
            ActorCell.<RuntimeException>throwUnchecked(interruption);
        }
    }

    /** 关闭后将尚未处理的系统消息和普通消息转交给 dead letters。 */
    public void cleanUp() {
        if (actor == null) return;
        Mailbox dlm = dispatcher().mailboxes.deadLetterMailbox();
        SystemMessage messageList = systemDrain(NoMessage.INSTANCE);
        while (messageList != null) {
            SystemMessage message = messageList;
            messageList = message.next();
            message.unlink();
            dlm.systemEnqueue(actor.self(), message);
        }
        if (messageQueue != null) messageQueue.cleanUp(actor.self(), dlm.messageQueue());
    }
}
