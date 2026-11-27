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

/**
 * 基于 {@link AbstractNodeQueue} 的多生产者单消费者无界消息队列。
 */
public class NodeMessageQueue extends AbstractNodeQueue<Envelope>
        implements UnboundedMessageQueueSemantics, MessageQueue {
    /** 创建 NodeMessageQueue 实例。 */
    public NodeMessageQueue() {
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
        return count();
    }

    @Override
    public boolean hasMessages() {
        return !isEmpty();
    }

    @Override
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        Envelope envelope;
        while ((envelope = dequeue()) != null) {
            deadLetters.enqueue(owner, envelope);
        }
    }
}
