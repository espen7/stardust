package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * DeathWatch 发送给 watcher 的终止通知消息。
 */
public final class Terminated implements Serializable {
    private final ActorRef actor;
    private final boolean existenceConfirmed;
    private final boolean addressTerminated;

    public Terminated(ActorRef actor) {
        this(actor, true, false);
    }

    public Terminated(ActorRef actor, boolean existenceConfirmed, boolean addressTerminated) {
        this.actor = actor;
        this.existenceConfirmed = existenceConfirmed;
        this.addressTerminated = addressTerminated;
    }

    public ActorRef actor() {
        return actor;
    }

    public ActorRef getActor() {
        return actor;
    }

    public boolean existenceConfirmed() {
        return existenceConfirmed;
    }

    public boolean addressTerminated() {
        return addressTerminated;
    }

    @Override
    public String toString() {
        return "Terminated(" + actor + ")";
    }
}
