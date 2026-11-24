package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

/**
 * 没有 {@link ActorCell} 的本地内部引用。
 *
 * <p>默认忽略普通消息和系统消息。子类可按所代表的引用类型覆写投递行为。
 */
public abstract class MinimalActorRef extends InternalActorRef {
    @Override
    public void tell(Object message, ActorRef sender) {
    }

    @Override
    public void sendSystemMessage(SystemMessage message) {
    }
}
