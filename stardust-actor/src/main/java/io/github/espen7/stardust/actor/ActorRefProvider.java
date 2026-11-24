package io.github.espen7.stardust.actor;

/**
 * ActorRef 和 ActorPath 的创建边界，对应 Pekko ActorRefProvider。
 */
public abstract class ActorRefProvider {

    public abstract ActorRef actorOf(Props props, String name, ActorRef parent);

    public abstract ActorRef rootGuardian();

    public abstract ActorRef deadLetters();
}
