package io.github.espen7.stardust.dispatch;

import io.github.espen7.stardust.actor.ActorRef;

/**
 * 普通用户消息队列的最小协议。Mailbox 不依赖具体队列实现。
 */
public interface MessageQueue {

    /**
     * 将消息放入指定接收方的普通消息队列。
     */
    void enqueue(ActorRef receiver, Envelope handle);

    /**
     * @return 队首消息；队列为空时返回 {@code null}
     */
    Envelope dequeue();

    /**
     * @return 队列非空时返回 {@code true}
     */
    boolean hasMessages();

    /**
     * 将队列中剩余消息交给 dead letters 队列。
     */
    void cleanUp(ActorRef owner, MessageQueue deadLetters);
}
