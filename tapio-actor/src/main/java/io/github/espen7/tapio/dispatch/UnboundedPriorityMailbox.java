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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * 基于 {@link PriorityBlockingQueue} 的无界优先级邮箱基类，对应 Pekko {@code UnboundedPriorityMailbox}。
 */
public class UnboundedPriorityMailbox implements MailboxType,
        ProducesMessageQueue<UnboundedPriorityMailbox.MessageQueue> {
    private final Comparator<Envelope> cmp;
    private final int initialCapacity;

    /**
     * 使用指定比较器与初始容量创建优先级邮箱类型。
     *
     * @param cmp 消息优先级比较器
     * @param initialCapacity 初始队列容量
     */
    public UnboundedPriorityMailbox(Comparator<Envelope> cmp, int initialCapacity) {
        if (initialCapacity < 1) {
            throw new IllegalArgumentException("initialCapacity must be >= 1");
        }
        this.cmp = Objects.requireNonNull(cmp, "cmp must not be null");
        this.initialCapacity = initialCapacity;
    }

    /**
     * 使用指定比较器和默认初始容量（11）创建优先级邮箱类型。
     *
     * @param cmp 消息优先级比较器
     */
    public UnboundedPriorityMailbox(Comparator<Envelope> cmp) {
        this(cmp, 11);
    }

    @Override
    public io.github.espen7.tapio.dispatch.MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue(initialCapacity, cmp);
    }

    /** 基于 {@link PriorityBlockingQueue} 的无界优先级消息队列。 */
    public static final class MessageQueue extends PriorityBlockingQueue<Envelope>
            implements UnboundedMessageQueueSemantics, io.github.espen7.tapio.dispatch.MessageQueue {
        /**
         * 创建无界优先级消息队列。
         *
         * @param initialCapacity 初始容量
         * @param comparator 优先级比较器
         */
        public MessageQueue(int initialCapacity, Comparator<Envelope> comparator) {
            super(initialCapacity, comparator);
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            add(handle);
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
