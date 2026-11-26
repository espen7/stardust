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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 优先处理 {@link ControlMessage} 的无界邮箱类型，对应 Pekko {@code UnboundedControlAwareMailbox}。
 */
public final class UnboundedControlAwareMailbox implements MailboxType,
        ProducesMessageQueue<UnboundedControlAwareMailbox.MessageQueue> {
    /** 创建控制感知无界邮箱类型实例。 */
    public UnboundedControlAwareMailbox() {
    }

    /**
     * 按反射邮箱契约创建控制感知无界邮箱类型实例。
     *
     * @param settings 系统设置
     * @param config 邮箱配置
     */
    public UnboundedControlAwareMailbox(ActorSystem.Settings settings, Config config) {
    }

    @Override
    public io.github.espen7.tapio.dispatch.MessageQueue create(ActorRef owner, ActorSystem system) {
        return new MessageQueue();
    }

    /** 维护独立控制队列与普通队列的无界控制感知消息队列。 */
    public static final class MessageQueue implements UnboundedMessageQueueSemantics,
            io.github.espen7.tapio.dispatch.MessageQueue {
        private final Queue<Envelope> controlQueue = new ConcurrentLinkedQueue<>();
        private final Queue<Envelope> queue = new ConcurrentLinkedQueue<>();

        /** 创建控制感知消息队列。 */
        public MessageQueue() {
        }

        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
            if (handle.message() instanceof ControlMessage) {
                controlQueue.add(handle);
            } else {
                queue.add(handle);
            }
        }

        @Override
        public Envelope dequeue() {
            Envelope control = controlQueue.poll();
            return control != null ? control : queue.poll();
        }

        @Override
        public int numberOfMessages() {
            return controlQueue.size() + queue.size();
        }

        @Override
        public boolean hasMessages() {
            return !(controlQueue.isEmpty() && queue.isEmpty());
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
