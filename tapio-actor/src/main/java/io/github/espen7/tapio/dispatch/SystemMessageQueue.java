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
import io.github.espen7.tapio.annotation.InternalStableApi;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

/** Mailbox 使用的系统消息队列协议。 */
interface SystemMessageQueue {
    @InternalStableApi
    void systemEnqueue(ActorRef receiver, SystemMessage message);

    SystemMessage systemDrain(SystemMessage newContents);

    boolean hasSystemMessages();
}
