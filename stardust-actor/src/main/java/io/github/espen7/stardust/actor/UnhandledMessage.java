package io.github.espen7.stardust.actor;

import java.util.Objects;

/**
 * Actor 未处理的用户消息，由 ActorSystem EventStream 发布。
 */
public record UnhandledMessage(Object message, ActorRef sender, ActorRef recipient) {
    public UnhandledMessage {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(sender, "sender");
        Objects.requireNonNull(recipient, "recipient");
    }
}
