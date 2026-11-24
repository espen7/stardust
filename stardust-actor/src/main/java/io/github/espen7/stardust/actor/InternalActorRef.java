package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

/**
 * 支持 Provider 查询和系统消息投递的 runtime 内部引用。
 */
public abstract class InternalActorRef extends ActorRef {
    /**
     * 返回引用的 Provider。
     *
     * @return 创建此引用的 {@link ActorRefProvider}
     */
    public abstract ActorRefProvider provider();

    /**
     * 向此引用投递系统消息。
     *
     * @param message 尚未链接至任何 Mailbox 的系统消息实例
     */
    public abstract void sendSystemMessage(SystemMessage message);
}
