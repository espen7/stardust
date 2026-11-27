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
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.InternalActorRef;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 默认有界 mailbox type。
 */
public final class BoundedMailbox implements MailboxType,
        ProducesMessageQueue<BoundedMailbox.MessageQueue>, ProducesPushTimeoutSemanticsMailbox {
    private final int capacity;
    private final Duration pushTimeOut;

    /**
     * 从配置创建有界 mailbox type。
     *
     * @param settings 所属系统的有效配置
     * @param config 已合并默认值的 mailbox 配置
     */
    public BoundedMailbox(ActorSystem.Settings settings, Config config) {
        this(config.getInt("mailbox-capacity"), config.getDuration("mailbox-push-timeout-time"));
    }

    /**
     * 创建有界 mailbox type。
     *
     * @param capacity 队列容量
     * @param pushTimeOut 入队超时；负值表示无限等待
     */
    public BoundedMailbox(int capacity, Duration pushTimeOut) {
        if (capacity < 0) throw new IllegalArgumentException("The capacity for BoundedMailbox can not be negative");
        if (pushTimeOut == null)
            throw new IllegalArgumentException("The push time-out for BoundedMailbox can not be null");
        this.capacity = capacity;
        this.pushTimeOut = pushTimeOut;
    }

    @Override
    public Duration pushTimeOut() {
        return pushTimeOut;
    }

    @Override
    public MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue(capacity, pushTimeOut);
    }

    /**
     * 容量固定的普通消息队列，队列满时按超时策略入队或发布 dead letter。
     */
    public static final class MessageQueue extends LinkedBlockingQueue<Envelope>
            implements BoundedQueueBasedMessageQueue {
        private final Duration pushTimeOut;

        /**
         * 创建有界队列。
         *
         * @param capacity 容量
         * @param pushTimeOut 入队超时
         */
        public MessageQueue(int capacity, Duration pushTimeOut) {
            super(capacity);
            this.pushTimeOut = pushTimeOut;
        }

        @Override
        public BlockingQueue<Envelope> queue() {
            return this;
        }

        @Override
        public Duration pushTimeOut() {
            return pushTimeOut;
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            try {
                if (!pushTimeOut.isNegative()) {
                    long timeoutNanos = pushTimeOut.getSeconds() >= Long.MAX_VALUE / 1_000_000_000L
                            ? Long.MAX_VALUE : pushTimeOut.toNanos();
                    boolean offered = offer(handle, timeoutNanos, TimeUnit.NANOSECONDS);
                    if (!offered) deadLetter(receiver, handle);
                } else {
                    put(handle);
                }
            } catch (InterruptedException error) {
                MessageQueue.<RuntimeException>throwUnchecked(error);
            }
        }

        @Override
        public Envelope dequeue() {
            return poll();
        }

        @Override
        public int numberOfMessages() {
            return size();
        }

        @Override
        public boolean hasMessages() {
            return !isEmpty();
        }

        @Override
        public void cleanUp(ActorRef owner, io.github.espen7.tapio.dispatch.MessageQueue deadLetters) {
            Envelope message;
            while ((message = dequeue()) != null) deadLetters.enqueue(owner, message);
        }

        private static void deadLetter(ActorRef receiver, Envelope handle) {
            ((InternalActorRef) receiver).provider().deadLetters().tell(
                    new DeadLetter(handle.message(), handle.sender(), receiver), handle.sender());
        }

        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void throwUnchecked(Throwable error) throws E {
            throw (E) error;
        }
    }
}
