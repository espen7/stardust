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
package io.github.espen7.stardust.dispatch;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorSystem;
import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Dispatcher 将 Envelope 或 SystemMessage 放入目标 Actor 的 Mailbox，并把 Mailbox
 * 提交给 Executor。它不直接调用 Actor，Actor 的串行执行由 Mailbox 的 Scheduled
 * 状态保证。
 */
public class Dispatcher implements AutoCloseable {
    private final ExecutorService executorService;
    private final DeadLetterMailbox deadLetterMailbox;
    private final int throughput;

    /** 创建使用默认 throughput 5 的 Dispatcher。 */
    public Dispatcher() {
        this(5);
    }

    /**
     * 创建使用指定 mailbox throughput 的 Dispatcher。
     *
     * @param throughput 每次 mailbox task 最多连续处理的普通消息数
     */
    public Dispatcher(int throughput) {
        this(throughput, null);
    }

    /**
     * 创建系统 Dispatcher。
     *
     * @param throughput 每次 Mailbox 调度最多连续处理的普通消息数
     * @param system 接收 dead-letter 事件的 ActorSystem。为 {@code null} 时使用本地消息队列
     */
    public Dispatcher(int throughput, ActorSystem system) {
        this.throughput = throughput;
        this.executorService = new ForkJoinPool(Math.max(1, Runtime.getRuntime().availableProcessors()));
        this.deadLetterMailbox = new DeadLetterMailbox(system);
    }

    /**
     * 返回每个 mailbox task 连续处理普通消息的上限。
     *
     * @return throughput 上限
     */
    public int throughput() {
        return throughput;
    }

    /**
     * 返回当前实现用作 dead letters sink 的消息队列。
     *
     * @return dead letters message queue
     */
    public MessageQueue deadLetters() {
        return deadLetterMailbox.messageQueue();
    }

    Mailbox deadLetterMailbox() {
        return deadLetterMailbox;
    }

    /**
     * 注销已终止的 Actor，并将旧 Mailbox 中的剩余消息转交给 dead letters。
     *
     * @param actor 待注销的 ActorCell
     */
    public void unregister(ActorCell actor) {
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
            executorService.execute(mailbox);
            return true;
        } catch (java.util.concurrent.RejectedExecutionException firstFailure) {
            try {
                executorService.execute(mailbox);
                return true;
            } catch (java.util.concurrent.RejectedExecutionException secondFailure) {
                mailbox.setAsIdle();
                throw secondFailure;
            }
        }
    }

    /** 关闭 Executor，停止接收新的 mailbox task。 */
    /** 关闭底层 Executor。 */
    @Override
    public void close() {
        executorService.shutdown();
    }
}
