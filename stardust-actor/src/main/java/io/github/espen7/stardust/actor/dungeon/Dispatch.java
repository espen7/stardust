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
package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.dispatch.Envelope;

/**
 * Pekko dungeon.Dispatch 的 Java 对照入口。
 *
 * <p>Dispatch 负责把消息封装并交给 Dispatcher，不在调用线程直接运行 Actor。
 * 这样 sendMessage 返回时，接收端的 receive 通常尚未执行。</p>
 */
public final class Dispatch {
    private Dispatch() {
    }

    /**
     * 从 message 和 sender 构造 Envelope 后开始异步投递。
     *
     * @param cell 当前 ActorCell
     * @param message 用户消息
     * @param sender 消息 sender
     */
    public static void sendMessage(ActorCell cell, Object message, ActorRef sender) {
        cell.sendMessage(message, sender);
    }

    /**
     * 转发已经构造好的 Envelope。
     *
     * @param cell 当前 ActorCell
     * @param message 待投递的 Envelope
     */
    public static void sendMessage(ActorCell cell, Envelope message) {
        cell.sendMessage(message.message(), message.sender());
    }
}
