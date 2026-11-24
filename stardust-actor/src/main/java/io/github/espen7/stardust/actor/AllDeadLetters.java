package io.github.espen7.stardust.actor;

/**
 * EventStream 上 dead-letter 事件的共同订阅类型。
 */
public interface AllDeadLetters extends WrappedMessage {
    /**
     * 返回未投递消息的发送者。
     *
     * @return 消息原始发送者，或表示未指定发送者的 dead letters 引用
     */
    ActorRef sender();

    /**
     * 返回未投递消息的目标。
     *
     * @return 消息原始接收者
     */
    ActorRef recipient();
}
