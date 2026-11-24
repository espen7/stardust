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

import io.github.espen7.stardust.actor.ActorRef;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 使用 ConcurrentLinkedQueue 的默认 unbounded message queue。
 */
public final class UnboundedMailbox implements MessageQueue {
    private final ConcurrentLinkedQueue<Envelope> queue = new ConcurrentLinkedQueue<>();

    /**
     * 不阻塞发送者地追加一条 Envelope。
     */
    @Override
    public void enqueue(ActorRef receiver, Envelope handle) {
        queue.add(handle);
    }

    /**
     * 从队首取出一条 Envelope；当前没有消息时返回 {@code null}。
     */
    @Override
    public Envelope dequeue() {
        return queue.poll();
    }

    /**
     * @return 普通消息队列非空时返回 {@code true}
     */
    @Override
    public boolean hasMessages() {
        return !queue.isEmpty();
    }

    /**
     * 将尚未处理的 Envelope 按队列顺序转交给 dead letters。
     */
    @Override
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        Envelope message;
        while ((message = dequeue()) != null) deadLetters.enqueue(owner, message);
    }
}
