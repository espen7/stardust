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

import io.github.espen7.tapio.actor.dungeon.ChildrenContainer;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

import java.util.Optional;

/** ActorRef 在完成或等待初始化时使用的消息投递接口。 */
@InternalApi
public interface Cell {
    ActorRef self();

    ActorSystem system();

    ActorSystemImpl systemImpl();

    Cell start();

    void suspend();

    void resume(Throwable causedByFailure);

    void restart(Throwable cause);

    void stop();

    InternalActorRef parent();

    ChildrenContainer childrenRefs();

    Optional<? extends ChildStats> getChildByName(String name);

    InternalActorRef getSingleChild(String name);

    boolean isLocal();

    boolean hasMessages();

    int numberOfMessages();

    Props props();

    /** 将用户消息封装成 Envelope 后投递。 */
    default void sendMessage(Object message, ActorRef sender) {
        sendMessage(Envelope.apply(message, sender, system()));
    }

    /**
     * 向当前 cell 投递普通消息。
     *
     * @param message 已包含 sender 的 Envelope
     */
    void sendMessage(Envelope message);

    /**
     * 向当前 cell 投递新的系统消息。
     *
     * @param message 尚未链接到队列的系统消息
     */
    void sendSystemMessage(SystemMessage message);

    /** @return Actor 终止后为 true */
    boolean isTerminated();
}
