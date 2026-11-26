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
 * 标记支持从队头重新入队的双端消息队列语义，供 {@link io.github.espen7.tapio.actor.AbstractActorWithStash}
 * 等暂存机制使用，对应 Pekko {@code DequeBasedMessageQueueSemantics}。
 */
public interface DequeBasedMessageQueueSemantics extends MessageQueue {
    /**
     * 将消息插入队列头部。
     *
     * @param receiver 接收消息的 Actor
     * @param handle 消息信封
     */
    void enqueueFirst(ActorRef receiver, Envelope handle);
}
