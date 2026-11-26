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
import io.github.espen7.tapio.dispatch.sysmsg.NoMessage;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

/** Dispatcher 创建普通 Mailbox 时使用的侵入式系统消息队列。 */
abstract class DefaultSystemMessageQueue extends Mailbox {
    DefaultSystemMessageQueue(MessageQueue messageQueue) {
        super(messageQueue);
    }

    @InternalStableApi
    @Override
    public final void systemEnqueue(ActorRef receiver, SystemMessage message) {
        for (;;) {
            assert message.unlinked();
            SystemMessage current = systemQueueGet();
            if (current == NoMessage.INSTANCE) {
                if (actor() != null) dispatcher().mailboxes.deadLetterMailbox().systemEnqueue(receiver, message);
                return;
            }
            message.next(current);
            if (systemQueuePut(current, message)) return;
            message.unlink();
        }
    }

    @Override
    public final SystemMessage systemDrain(SystemMessage newContents) {
        for (;;) {
            SystemMessage current = systemQueueGet();
            if (current == NoMessage.INSTANCE) return null;
            if (!systemQueuePut(current, newContents)) continue;
            SystemMessage reversed = null;
            while (current != null) {
                SystemMessage next = current.next();
                current.next(reversed);
                reversed = current;
                current = next;
            }
            return reversed;
        }
    }

    @Override
    public boolean hasSystemMessages() {
        SystemMessage head = systemQueueGet();
        return head != null && head != NoMessage.INSTANCE;
    }
}
