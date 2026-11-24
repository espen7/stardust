package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.dispatch.Envelope;

/**
 * Pekko dungeon.Dispatch 的 Java 对照入口。
 *
 * <p>Dispatch 负责把消息封装并交给 Dispatcher，不在调用线程直接运行 Actor。
 * 这样 sendMessage 返回时，接收端的 receive 通常尚未执行。</p>
 */
public final class Dispatch {
    private Dispatch() {
    }

    /**
     * 从 message 和 sender 构造 Envelope 后开始异步投递。
     */
    public static void sendMessage(ActorCell cell, Object message, ActorRef sender) {
        cell.sendMessage(message, sender);
    }

    /**
     * 转发已经构造好的 Envelope。
     */
    public static void sendMessage(ActorCell cell, Envelope message) {
        cell.sendMessage(message.message(), message.sender());
    }
}
