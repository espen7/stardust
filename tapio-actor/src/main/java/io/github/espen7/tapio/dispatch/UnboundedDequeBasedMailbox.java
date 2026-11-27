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

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 基于 {@link LinkedBlockingDeque} 的无界双端邮箱类型。
 */
public final class UnboundedDequeBasedMailbox implements MailboxType,
        ProducesMessageQueue<UnboundedDequeBasedMailbox.MessageQueue> {
    /** 创建无界双端邮箱类型实例。 */
    public UnboundedDequeBasedMailbox() {
    }

    /**
     * 按反射邮箱契约创建无界双端邮箱类型实例。
     *
     * @param settings 系统设置
     * @param config 邮箱配置
     */
    public UnboundedDequeBasedMailbox(ActorSystem.Settings settings, Config config) {
    }

    @Override
    public io.github.espen7.tapio.dispatch.MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue();
    }

    /** 基于 {@link LinkedBlockingDeque} 的无界双端消息队列实现。 */
    public static final class MessageQueue extends LinkedBlockingDeque<Envelope>
            implements DequeBasedMessageQueue, UnboundedDequeBasedMessageQueueSemantics {
        /** 创建无界双端消息队列。 */
        public MessageQueue() {
        }

        @Override
        public Deque<Envelope> queue() {
            return this;
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            add(handle);
        }

        @Override
        public void enqueueFirst(ActorRef receiver, Envelope handle) {
            addFirst(handle);
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
