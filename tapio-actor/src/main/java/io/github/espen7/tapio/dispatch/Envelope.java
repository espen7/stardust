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
import io.github.espen7.tapio.actor.InvalidMessageException;

/**
 * 一条用户消息及其 sender，等价于 Pekko dispatch.Envelope。
 *
 * @param message 用户消息，不允许为 {@code null}
 * @param sender  发送者引用；未指定时由 {@link #apply(Object, ActorRef, ActorSystem)} 使用 dead letters
 */
public record Envelope(Object message, ActorRef sender) {
    /**
     * 创建 Envelope，并把 null sender 规范化为 system.deadLetters。
     *
     * @param message 要投递的用户消息
     * @param sender  消息发送者，可以为 {@code null}
     * @param system  接收者所属的 ActorSystem
     * @return 可放入 Mailbox 的 Envelope
     * @throws InvalidMessageException 当 message 为 {@code null}
     */
    public static Envelope apply(Object message, ActorRef sender, ActorSystem system) {
        if (message == null) throw new InvalidMessageException("Message is null");
        return new Envelope(message, sender == null ? system.deadLetters() : sender);
    }
}
