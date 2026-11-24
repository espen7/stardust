package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * DeathWatch 发送给 watcher 的终止通知消息。
 */
public final class Terminated implements Serializable {
    private final ActorRef actor;

    public Terminated(ActorRef actor) {
        this.actor = actor;
    }

    public ActorRef actor() {
        return actor;
    }

    public ActorRef getActor() {
        return actor;
    }

    @Override
    public String toString() {
        return "Terminated(" + actor + ")";
    }
}
