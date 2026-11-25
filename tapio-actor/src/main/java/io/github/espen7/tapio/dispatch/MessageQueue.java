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
 * 普通用户消息队列的最小协议。Mailbox 不依赖具体队列实现。
 */
public interface MessageQueue {

    /**
     * 将消息放入指定接收方的普通消息队列。
     */
    void enqueue(ActorRef receiver, Envelope handle);

    /**
     * @return 队首消息；队列为空时返回 {@code null}
     */
    Envelope dequeue();

    /**
     * @return 队列非空时返回 {@code true}
     */
    boolean hasMessages();

    /**
     * 将队列中剩余消息交给 dead letters 队列。
     */
    void cleanUp(ActorRef owner, MessageQueue deadLetters);
}
