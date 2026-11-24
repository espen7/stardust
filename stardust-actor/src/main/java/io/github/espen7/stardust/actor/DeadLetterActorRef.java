package io.github.espen7.stardust.actor;

/**
 * DeadLetterActorRef 吸收已经无法投递的消息。
 * 当前版本不发布 DeadLetter event，但保留独立 ActorRef 以维持 sender 和路径语义。
 */
public final class DeadLetterActorRef extends ActorRef {
    private final ActorPath path;

    public DeadLetterActorRef(ActorPath path) {
        this.path = path;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public String toString() {
        return "DeadLetterActorRef(" + path + ")";
    }
}
