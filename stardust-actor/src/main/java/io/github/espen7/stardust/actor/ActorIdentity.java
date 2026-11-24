package io.github.espen7.stardust.actor;

import java.io.Serializable;
import java.util.Optional;

/**
 * {@link Identify} 的回复。
 *
 * @param correlationId 请求中的关联标识
 * @param ref 已找到的 Actor 引用；未找到时为空
 */
public record ActorIdentity(Object correlationId, Optional<ActorRef> ref) implements Serializable {
    /**
     * 以 Java Optional 返回找到的引用。
     *
     * @return 找到的 Actor 引用，未找到时为空
     */
    public Optional<ActorRef> getActorRef() {
        return ref;
    }
}
