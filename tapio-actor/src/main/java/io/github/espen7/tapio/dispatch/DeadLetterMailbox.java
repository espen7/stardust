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
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;

/**
 * 已终止的 Actor 共用的 Mailbox，将无法投递的消息转发给 dead letters。
 */
final class DeadLetterMailbox extends Mailbox {
    private final ActorRef deadLetters;

    DeadLetterMailbox(ActorRef deadLetters) {
        super(new MessageQueue() {
            @Override
            public void enqueue(ActorRef receiver, Envelope envelope) {
                if (!(envelope.message() instanceof DeadLetter)) {
                    deadLetters.tell(new DeadLetter(envelope.message(), envelope.sender(), receiver), envelope.sender());
                }
            }

            @Override
            public Envelope dequeue() {
                return null;
            }

            @Override
            public boolean hasMessages() {
                return false;
            }

            @Override
            public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
            }
        });
        this.deadLetters = deadLetters;
        close();
    }

    @Override
    public void systemEnqueue(ActorRef receiver, SystemMessage message) {
        deadLetters.tell(new DeadLetter(message, receiver, receiver));
    }
}
