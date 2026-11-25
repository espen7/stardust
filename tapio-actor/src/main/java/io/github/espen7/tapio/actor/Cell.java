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

import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

/** ActorRef 在完成或等待初始化时使用的消息投递接口。 */
public interface Cell {
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
