package io.github.espen7.stardust.pattern;

import io.github.espen7.stardust.actor.ActorPath;
import io.github.espen7.stardust.actor.ActorRef;

import java.util.concurrent.CompletableFuture;

/**
 * ask 模式的临时回复引用，tell 会尝试完成对应的 CompletableFuture。
 */
public final class PromiseActorRef extends ActorRef {
    private final ActorPath path;
    private final CompletableFuture<Object> result;

    public PromiseActorRef(ActorPath path, CompletableFuture<Object> result) {
        this.path = path;
        this.result = result;
    }

    @Override
    public ActorPath path() {
        return path;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        result.complete(message);
    }

    @Override
    public boolean isTerminated() {
        return result.isDone();
    }
}
