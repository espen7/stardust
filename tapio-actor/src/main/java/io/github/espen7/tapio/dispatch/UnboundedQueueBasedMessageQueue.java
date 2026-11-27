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
 * 结合 {@link QueueBasedMessageQueue} 与 {@link UnboundedMessageQueueSemantics} 的无界队列接口。
 */
public interface UnboundedQueueBasedMessageQueue extends QueueBasedMessageQueue, UnboundedMessageQueueSemantics {

    @Override
    default void enqueue(ActorRef receiver, Envelope handle) {
        queue().add(handle);
    }

    @Override
    default Envelope dequeue() {
        return queue().poll();
    }
}
