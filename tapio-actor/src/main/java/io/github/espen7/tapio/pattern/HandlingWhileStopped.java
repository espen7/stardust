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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.annotation.InternalApi;

/**
 * 子 Actor 处于退避停止期间收到消息时的处理方式，对应 Pekko {@code HandlingWhileStopped}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public sealed interface HandlingWhileStopped
        permits HandlingWhileStopped.ForwardDeathLetters, HandlingWhileStopped.ForwardTo, HandlingWhileStopped.ReplyWith {
    /** 转发给系统 dead letters。 */
    final class ForwardDeathLetters implements HandlingWhileStopped {
        public static final ForwardDeathLetters INSTANCE = new ForwardDeathLetters();

        private ForwardDeathLetters() {
        }
    }

    /** 转发给指定处理 Actor。 */
    record ForwardTo(ActorRef handler) implements HandlingWhileStopped {
    }

    /** 直接向发送者回复固定消息。 */
    record ReplyWith(Object replyMessage) implements HandlingWhileStopped {
    }
}
