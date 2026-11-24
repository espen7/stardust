package io.github.espen7.stardust.actor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地 ActorRefProvider。
 *
 * <p>Provider 负责生成 ActorPath、分配 incarnation UID，并把新 Actor 挂到 parent
 * 的 children 集合。当前实现只支持同一 JVM 的 LocalActorRef，未来 Remote provider
 * 可以复用 ActorRefProvider 的抽象边界。</p>
 */
public class LocalActorRefProvider extends ActorRefProvider {
    private final ActorSystemImpl system;
    // UID 区分同一路径上的不同 Actor incarnation。
    private final AtomicInteger uidGenerator = new AtomicInteger(1);
    private final LocalActorRef rootGuardian;
    private final DeadLetterActorRef deadLetters;

    public LocalActorRefProvider(ActorSystemImpl system) {
        this.system = system;
        this.deadLetters = new DeadLetterActorRef(new ActorPath("pekko://" + system.name() + "/deadLetters"));
        this.rootGuardian = new LocalActorRef(system, Props.create(EmptyActor.class), system.dispatcher(), null,
                new ActorPath("pekko://" + system.name() + "/user", uidGenerator.getAndIncrement()), "user");
    }

    /**
     * 创建 LocalActorRef，并异步向其 Mailbox 投递 Create。
     */
    @Override
    public ActorRef actorOf(Props props, String name, ActorRef parent) {
        if (name == null || name.isBlank() || name.contains("/"))
            throw new IllegalArgumentException("Invalid actor name: " + name);
        String base = parent == null ? rootGuardian.path().toString() : parent.path().toString();
        LocalActorRef result = new LocalActorRef(system, props, system.dispatcher(), parent == null ? rootGuardian : parent,
                new ActorPath(base + "/" + name, uidGenerator.getAndIncrement()), name);
        return result;
    }

    @Override
    public ActorRef rootGuardian() {
        return rootGuardian;
    }

    @Override
    public ActorRef deadLetters() {
        return deadLetters;
    }

    private static final class EmptyActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
    }
}
