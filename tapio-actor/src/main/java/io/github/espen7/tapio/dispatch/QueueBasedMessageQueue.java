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

import java.util.Queue;

/**
 * 由 {@link Queue} 支持的消息队列基接口，对应上游 {@code QueueBasedMessageQueue}。
 */
public interface QueueBasedMessageQueue extends MessageQueue, MultipleConsumerSemantics {

    /**
     * 返回底层消息队列实例。
     *
     * @return 底层队列
     */
    Queue<Envelope> queue();

    @Override
    default int numberOfMessages() {
        return queue().size();
    }

    @Override
    default boolean hasMessages() {
        return !queue().isEmpty();
    }

    @Override
    default void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        if (hasMessages()) {
            Envelope envelope = dequeue();
            while (envelope != null) {
                deadLetters.enqueue(owner, envelope);
                envelope = dequeue();
            }
        }
    }
}
