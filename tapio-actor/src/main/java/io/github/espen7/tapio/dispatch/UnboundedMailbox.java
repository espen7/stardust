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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 使用 ConcurrentLinkedQueue 的默认 unbounded mailbox type，对应上游
 * {@code dispatch.UnboundedMailbox}。
 */
public final class UnboundedMailbox implements MailboxType, ProducesMessageQueue<UnboundedMailbox.MessageQueue> {

    /** 不使用构造参数创建默认 mailbox type。 */
    public UnboundedMailbox() {
    }

    /**
     * 供配置反射构造使用，参数与上游辅助构造一致。
     *
     * @param settings 所属系统的有效配置
     * @param config 已合并默认值的 mailbox 配置
     */
    public UnboundedMailbox(ActorSystem.Settings settings, com.typesafe.config.Config config) {
        this();
    }

    @Override
    public MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue();
    }

    /**
     * 不限制容量的普通消息队列。
     */
    public static class MessageQueue
            implements io.github.espen7.tapio.dispatch.MessageQueue, UnboundedMessageQueueSemantics {
        private final ConcurrentLinkedQueue<Envelope> queue = new ConcurrentLinkedQueue<>();

        /** 创建空队列。 */
        public MessageQueue() {
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            queue.add(handle);
        }

        @Override
        public Envelope dequeue() {
            return queue.poll();
        }

        @Override
        public boolean hasMessages() {
            return !queue.isEmpty();
        }

        @Override
        public void cleanUp(ActorRef owner, io.github.espen7.tapio.dispatch.MessageQueue deadLetters) {
            Envelope message;
            while ((message = dequeue()) != null) deadLetters.enqueue(owner, message);
        }
    }
}
