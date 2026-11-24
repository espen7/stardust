package io.github.espen7.stardust.dispatch;

import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.actor.ActorSystem;
import io.github.espen7.stardust.actor.DeadLetter;
import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

/**
 * 已终止的 Actor 共用的 Mailbox，将无法投递的消息转发给 dead letters。
 */
final class DeadLetterMailbox extends Mailbox {
    private final ActorSystem system;

    DeadLetterMailbox(ActorSystem system) {
        super(system == null ? new UnboundedMailbox() : new MessageQueue() {
            @Override
            public void enqueue(ActorRef receiver, Envelope envelope) {
                if (!(envelope.message() instanceof DeadLetter)) {
                    system.deadLetters().tell(new DeadLetter(envelope.message(), envelope.sender(), receiver), envelope.sender());
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
        this.system = system;
        close();
    }

    @Override
    public void systemEnqueue(ActorRef receiver, SystemMessage message) {
        if (system != null) system.deadLetters().tell(new DeadLetter(message, receiver, receiver));
    }
}
