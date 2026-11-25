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
package io.github.espen7.tapio.actor;

import java.io.Serializable;

/**
 * 消息被丢弃时发布的事件。
 *
 * @param message 被丢弃的消息
 * @param reason 丢弃原因
 * @param sender 发送者，可以为 {@link ActorRef#noSender}
 * @param recipient 接收者
 */
public record Dropped(Object message, String reason, ActorRef sender, ActorRef recipient)
        implements AllDeadLetters, Serializable {
}
