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
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.InternalActorRef;

import java.time.Duration;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 基于 {@link LinkedBlockingDeque} 的有界双端邮箱类型。
 */
public final class BoundedDequeBasedMailbox implements MailboxType,
        ProducesMessageQueue<BoundedDequeBasedMailbox.MessageQueue>,
        ProducesPushTimeoutSemanticsMailbox {
    private final int capacity;
    private final Duration pushTimeOut;

    /**
     * 创建有界双端邮箱类型。
     *
     * @param capacity 队列容量（非负）
     * @param pushTimeOut 入队超时（负值表示无限等待）
     */
    public BoundedDequeBasedMailbox(int capacity, Duration pushTimeOut) {
        if (capacity < 0) {
            throw new IllegalArgumentException("The capacity for BoundedDequeBasedMailbox can not be negative");
        }
        if (pushTimeOut == null) {
            throw new IllegalArgumentException("The push time-out for BoundedDequeBasedMailbox can not be null");
        }
        this.capacity = capacity;
        this.pushTimeOut = pushTimeOut;
    }

    /**
     * 按反射邮箱契约创建有界双端邮箱类型。
     *
     * @param settings 系统设置
     * @param config 邮箱配置
     */
    public BoundedDequeBasedMailbox(ActorSystem.Settings settings, Config config) {
        this(config.getInt("mailbox-capacity"), Duration.ofNanos(config.getDuration("mailbox-push-timeout-time").toNanos()));
    }

    @Override
    public Duration pushTimeOut() {
        return pushTimeOut;
    }

    @Override
    public io.github.espen7.tapio.dispatch.MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue(capacity, pushTimeOut);
    }

    /** 基于 {@link LinkedBlockingDeque} 的有界双端消息队列实现。 */
    public static final class MessageQueue extends LinkedBlockingDeque<Envelope>
            implements DequeBasedMessageQueue, BoundedDequeBasedMessageQueueSemantics {
        private final Duration pushTimeOut;

        /**
         * 创建有界双端消息队列。
         *
         * @param capacity 队列容量
         * @param pushTimeOut 入队超时
         */
        public MessageQueue(int capacity, Duration pushTimeOut) {
            super(capacity);
            this.pushTimeOut = pushTimeOut;
        }

        @Override
        public Deque<Envelope> queue() {
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
                    if (!offer(handle, pushTimeOut.toNanos(), TimeUnit.NANOSECONDS)) {
                        publishDeadLetter(receiver, handle);
                    }
                } else {
                    put(handle);
                }
            } catch (InterruptedException interrupted) {
                ActorCell.<RuntimeException>throwUnchecked(interrupted);
            }
        }

        @Override
        public void enqueueFirst(ActorRef receiver, Envelope handle) {
            try {
                if (!pushTimeOut.isNegative()) {
                    if (!offerFirst(handle, pushTimeOut.toNanos(), TimeUnit.NANOSECONDS)) {
                        publishDeadLetter(receiver, handle);
                    }
                } else {
                    putFirst(handle);
                }
            } catch (InterruptedException interrupted) {
                ActorCell.<RuntimeException>throwUnchecked(interrupted);
            }
        }

        private static void publishDeadLetter(ActorRef receiver, Envelope handle) {
            ((InternalActorRef) receiver).provider().deadLetters().tell(
                    new DeadLetter(handle.message(), handle.sender(), receiver), handle.sender());
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
            Envelope envelope;
            while ((envelope = dequeue()) != null) {
                deadLetters.enqueue(owner, envelope);
            }
        }
    }
}
