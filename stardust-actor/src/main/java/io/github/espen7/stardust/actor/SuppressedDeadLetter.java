package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * 在标有 {@link DeadLetterSuppression} 的消息投递失败时发布的事件。
 *
 * @param message 原始消息
 * @param sender 原始发送者。没有显式发送者时，runtime 使用所属系统的 dead letters 引用
 * @param recipient 原始接收者
 */
public record SuppressedDeadLetter(DeadLetterSuppression message, ActorRef sender, ActorRef recipient)
        implements AllDeadLetters, Serializable {
    /**
     * 保存无法投递的消息及其原始发送者和接收者。
     *
     * @param message 原始消息
     * @param sender 原始发送者
     * @param recipient 原始接收者
     * @throws IllegalArgumentException 发送者或接收者为 {@code null}
     */
    public SuppressedDeadLetter {
        if (sender == null) throw new IllegalArgumentException("requirement failed: DeadLetter sender may not be null");
        if (recipient == null) throw new IllegalArgumentException("requirement failed: DeadLetter recipient may not be null");
    }
}
