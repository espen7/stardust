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
package io.github.espen7.stardust.actor;

import java.util.Objects;

/**
 * Actor 未处理的用户消息，由 ActorSystem EventStream 发布。
 *
 * @param message 未处理的用户消息
 * @param sender 消息 sender
 * @param recipient 未处理该消息的 Actor
 */
public record UnhandledMessage(Object message, ActorRef sender, ActorRef recipient) implements AllDeadLetters {
    public UnhandledMessage {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(sender, "sender");
        Objects.requireNonNull(recipient, "recipient");
    }
}
