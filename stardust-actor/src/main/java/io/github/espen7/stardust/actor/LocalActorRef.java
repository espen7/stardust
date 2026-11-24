package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.dispatch.Dispatcher;
import io.github.espen7.stardust.dispatch.sysmsg.SystemMessage;

/**
 * 本地 ActorRef，持有一个 ActorCell。
 *
 * <p>LocalActorRef 是公开引用和内部执行状态之间的桥梁。它不直接调用 Actor，
 * tell 和 stop 都转成 ActorCell 的消息或系统消息。</p>
 */
public class LocalActorRef extends ActorRef {
    private final ActorSystemImpl system;
    private final ActorPath path;
    private final ActorCell actorCell;
    private final String name;

    /**
     * 构造完成后立即创建 ActorCell，并触发异步初始化。
     */
    public LocalActorRef(ActorSystemImpl system, Props props, Dispatcher dispatcher, ActorRef supervisor, ActorPath path, String name) {
        this.system = system;
        this.path = path;
        this.name = name;
        this.actorCell = newActorCell(system, this, props, dispatcher, supervisor);
        actorCell.init(true);
    }

    /**
     * 可覆写的 ActorCell factory，对应 Pekko LocalActorRef.newActorCell。
     */
    protected ActorCell newActorCell(ActorSystemImpl system, LocalActorRef ref, Props props, Dispatcher dispatcher, ActorRef supervisor) {
        return new ActorCell(system, ref, props, dispatcher, supervisor);
    }

    public ActorCell underlying() {
        return actorCell;
    }

    public String name() {
        return name;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    /**
     * 将用户消息交给 ActorCell，实际执行由 Dispatcher 和 Mailbox 完成。
     */
    @Override
    public void tell(Object message, ActorRef sender) {
        actorCell.sendMessage(message, sender);
    }

    @Override
    public void stop() {
        actorCell.sendSystemMessage(new SystemMessage.Terminate());
    }

    @Override
    public boolean isTerminated() {
        return actorCell.isTerminated();
    }
}
