package io.github.espen7.stardust.actor;

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
