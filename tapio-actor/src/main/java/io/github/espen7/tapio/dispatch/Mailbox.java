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
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Actor 的双队列 Mailbox。
 *
 * <p>普通消息保存在 MessageQueue，系统消息直接链接在 SystemMessage 对象上。
 * 系统消息队列使用头插，因此入队是 O(1)，执行前通过反转恢复 FIFO 顺序。
 * status 的位布局沿用 Pekko，Scheduled 位用于保证同一个 Mailbox 同时只有一个
 * Dispatcher task。</p>
 */
public class Mailbox implements Runnable {
    public static final int Open = 0;
    public static final int Closed = 1;
    public static final int Scheduled = 2;
    public static final int shouldScheduleMask = 3;
    public static final int shouldNotProcessMask = ~2;
    public static final int suspendMask = ~3;
    public static final int suspendUnit = 4;

    private final MessageQueue messageQueue;
    /* status 低位表示 Open/Closed，Scheduled 表示已经提交给 Executor。 */
    private final AtomicInteger status = new AtomicInteger(Open);
    /* 只保存系统消息链表的 head，消息本身就是链表节点，避免额外 node 分配。 */
    private final AtomicReference<SystemMessage> systemMessage = new AtomicReference<>();
    private volatile ActorCell actor;
    private Dispatcher dispatcher;

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
    public void setActor(ActorCell actor, Dispatcher dispatcher) {
        this.actor = actor;
        this.dispatcher = dispatcher;
    }

    /**
     * @return 此 Mailbox 所属的 ActorCell
     */
    public ActorCell actor() {
        return actor;
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

    /**
     * @return Mailbox 未关闭且 suspend count 为零时返回 {@code true}
     */
    public boolean shouldProcessMessage() {
        return (status.get() & shouldNotProcessMask) == 0;
    }

    /**
     * @return 当前嵌套 suspend 次数
     */
    public int suspendCount() {
        return status.get() / suspendUnit;
    }

    /**
     * @return suspend count 大于零时返回 {@code true}
     */
    public boolean isSuspended() {
        return (status.get() & suspendMask) != 0;
    }

    /**
     * @return Mailbox 已关闭时返回 {@code true}
     */
    public boolean isClosed() {
        return status.get() == Closed;
    }

    /**
     * @return Mailbox task 已提交或正在执行时返回 {@code true}
     */
    public boolean isScheduled() {
        return (status.get() & Scheduled) != 0;
    }

    /**
     * 将 suspend count 减一。
     *
     * @return 操作后 suspend count 为零时返回 {@code true}
     */
    public boolean resume() {
        for (; ; ) {
            int old = status.get();
            if (old == Closed) return false;
            int next = old < suspendUnit ? old : old - suspendUnit;
            if (status.compareAndSet(old, next)) return next < suspendUnit;
            Thread.onSpinWait();
        }
    }

    /**
     * 将 suspend count 加一。
     *
     * @return 操作前 suspend count 为零时返回 {@code true}
     */
    public boolean suspend() {
        for (; ; ) {
            int old = status.get();
            if (old == Closed) return false;
            if (status.compareAndSet(old, old + suspendUnit)) return old < suspendUnit;
            Thread.onSpinWait();
        }
    }

    /**
     * 原子地将 Mailbox 状态设为 Closed。
     *
     * @return 本次调用首次关闭 Mailbox 时返回 {@code true}
     */
    public boolean becomeClosed() {
        for (; ; ) {
            int old = status.get();
            if (old == Closed) return false;
            if (status.compareAndSet(old, Closed)) return true;
            Thread.onSpinWait();
        }
    }

    /**
     * CAS 设置 Scheduled，失败表示已经有 task 在运行或等待运行。
     *
     * @return 本次调用成功设置 Scheduled 时返回 {@code true}
     */
    public boolean setAsScheduled() {
        for (; ; ) {
            int old = status.get();
            if ((old & shouldScheduleMask) != Open) return false;
            if (status.compareAndSet(old, old | Scheduled)) return true;
            Thread.onSpinWait();
        }
    }

    /**
     * 清除 Scheduled，并返回这次是否真的改变了状态。
     *
     * @return 状态更新成功时返回 {@code true}
     */
    public boolean setAsIdle() {
        for (; ; ) {
            int old = status.get();
            if (status.compareAndSet(old, old & ~Scheduled)) return true;
            Thread.onSpinWait();
        }
    }

    /**
     * 将系统消息压入链表头，CAS 负责并发发送方之间的协调。
     *
     * @param receiver 系统消息的目标 ActorRef，用于匹配 Pekko 的 enqueue API
     * @param message  要入队的系统消息；消息对象不能同时链接到多个 Mailbox
     */
    public void systemEnqueue(ActorRef receiver, SystemMessage message) {
        for (; ; ) {
            SystemMessage old = systemMessage.get();
            if (old == SystemMessage.NoMessage) {
                dispatcher.deadLetterMailbox().systemEnqueue(receiver, message);
                break;
            }
            message.next(old);
            if (systemMessage.compareAndSet(old, message)) break;
            message.unlink();
        }
    }

    private SystemMessage systemDrain(SystemMessage newContents) {
        for (;;) {
            SystemMessage current = systemMessage.get();
            if (current == SystemMessage.NoMessage) return null;
            if (!systemMessage.compareAndSet(current, newContents)) continue;
            SystemMessage reversed = null;
            while (current != null) {
                SystemMessage next = current.next();
                current.next(reversed);
                reversed = current;
                current = next;
            }
            return reversed;
        }
    }

    /**
     * @return 系统消息队列非空时返回 {@code true}
     */
    public boolean hasMessages() {
        SystemMessage head = systemMessage.get();
        return head != null && head != SystemMessage.NoMessage;
    }

    /**
     * 根据状态位和队列提示判断 Mailbox 是否可以提交给 Executor。
     *
     * @param hasMessageHint       普通消息队列非空提示
     * @param hasSystemMessageHint 系统消息队列非空提示
     * @return 当前可调度时返回 {@code true}
     */
    public boolean canBeScheduledForExecution(boolean hasMessageHint, boolean hasSystemMessageHint) {
        int current = status.get();
        if (current == Open || current == Scheduled) {
            return hasMessageHint || hasSystemMessageHint || hasMessages() || messageQueue.hasMessages();
        }
        if (current == Closed) return false;
        return hasSystemMessageHint || hasMessages();
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
            if (dispatcher != null) dispatcher.registerForExecution(this, false, false);
        }
    }

    /**
     * 按 Dispatcher throughput 批量处理普通消息。
     */
    private void processMailbox() {
        int left = Math.max(dispatcher.throughput(), 1);
        boolean deadlineDefined = dispatcher.isThroughputDeadlineTimeDefined();
        long deadlineNs = deadlineDefined ? System.nanoTime() + dispatcher.throughputDeadlineTime().toNanos() : 0L;
        while (left-- > 0 && shouldProcessMessage()) {
            Envelope next = dequeue();
            if (next == null) break;
            actor.invoke(next);
            processAllSystemMessages();
            if (deadlineDefined && System.nanoTime() - deadlineNs >= 0) break;
        }
    }


    /**
     * 摘取系统消息队列并按入队顺序调用 ActorCell.systemInvoke。
     */
    public void processAllSystemMessages() {
        SystemMessage current = systemDrain(null);
        while (current != null && !isClosed()) {
            SystemMessage next = current.next();
            current.unlink();
            actor.systemInvoke(current);
            current = next;
        }
        forwardSystemMessages(current);
    }

    private void forwardSystemMessages(SystemMessage current) {
        while (current != null) {
            SystemMessage next = current.next();
            current.unlink();
            dispatcher.deadLetterMailbox().systemEnqueue(actor.self(), current);
            current = next;
        }
    }

    /**
     * 关闭后将尚未处理的系统消息和普通消息转交给 dead letters。
     */
    public void cleanUp() {
        forwardSystemMessages(systemDrain(SystemMessage.NoMessage));
        messageQueue.cleanUp(actor.self(), dispatcher.deadLetters());
    }

    /**
     * 将 Mailbox 状态改为 Closed。
     */
    public void close() {
        becomeClosed();
    }
}
