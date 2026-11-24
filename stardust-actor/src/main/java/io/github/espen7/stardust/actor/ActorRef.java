package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * Actor 的不可变引用。
 *
 * <p>ActorRef 只负责寻址和投递消息，不暴露 Actor 实例。调用者可以安全地
 * 在 Actor 之间传递引用。LocalActorRef 会把投递转给自己的 ActorCell，未来
 * RemoteActorRef 可以在同一接口上替换为网络投递。</p>
 */
public abstract class ActorRef implements Comparable<ActorRef>, Serializable {

    public static final ActorRef noSender = null;

    public abstract ActorPath path();

    /**
     * fire-and-forget 投递入口，sender 会被保存到 Envelope。
     */
    public abstract void tell(Object message, ActorRef sender);

    public final void tell(Object message) {
        tell(message, noSender);
    }

    /**
     * 保留原 sender 的 forward 操作。
     */
    public void forward(Object message, ActorContext context) {
        tell(message, context.sender());
    }

    public boolean isTerminated() {
        return false;
    }

    public void stop() {
    }

    @Override
    public int compareTo(ActorRef other) {
        return path().compareTo(other.path());
    }

    /**
     * 计算此引用的哈希码。
     *
     * @return 已定义的 Actor incarnation UID，或未定义 UID 时的路径哈希码
     */
    @Override
    public int hashCode() {
        ActorPath path = path();
        return path.uid() == ActorPath.UndefinedUid ? path.hashCode() : path.uid();
    }

    /**
     * Actor identity 同时包含路径和 UID。相同路径的新 incarnation 不会与旧引用相等。
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof ActorRef ref && path().equals(ref.path()) && path().uid() == ref.path().uid();
    }

    /**
     * 返回引用的诊断文本。
     *
     * @return Actor 路径及已定义的 incarnation UID
     */
    @Override
    public String toString() {
        ActorPath path = path();
        return path.uid() == ActorPath.UndefinedUid
                ? "Actor[" + path + "]"
                : "Actor[" + path + "#" + path.uid() + "]";
    }
}
