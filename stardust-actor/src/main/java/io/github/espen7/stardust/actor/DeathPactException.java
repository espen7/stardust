package io.github.espen7.stardust.actor;

/** 未处理 DeathWatch 终止通知时由 Actor 抛出的异常。 */
public final class DeathPactException extends RuntimeException {
    private final ActorRef dead;

    public DeathPactException(ActorRef dead) {
        super("Monitored actor [" + dead + "] terminated");
        this.dead = dead;
    }

    public ActorRef dead() {
        return dead;
    }
}
