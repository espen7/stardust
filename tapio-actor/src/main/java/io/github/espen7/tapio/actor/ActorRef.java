/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.actor;

import java.io.Serializable;

/**
 * Actor 的不可变引用。
 *
 * <p>ActorRef 只负责寻址和投递消息，不暴露 Actor 实例。调用者可以安全地
 * 在 Actor 之间传递引用。LocalActorRef 会把投递转给自己的 ActorCell，未来
 * RemoteActorRef 可以在同一接口上替换为网络投递。</p>
 */
public abstract class ActorRef implements Comparable<ActorRef>, Serializable {

    /** 仅供具体 ActorRef 实现调用的构造器。 */
    protected ActorRef() {
    }

    /** 未指定 sender 时使用的空引用。 */
    public static final ActorRef noSender = null;

    /**
     * 返回此引用的 ActorPath。
     *
     * @return ActorPath
     */
    public abstract ActorPath path();

    /**
     * fire-and-forget 投递入口，sender 会被保存到 Envelope。
     *
     * @param message 待投递的消息
     * @param sender 消息 sender，可以为 {@link #noSender}
     */
    public abstract void tell(Object message, ActorRef sender);

    /**
     * 使用空 sender 投递消息。
     *
     * @param message 待投递的消息
     */
    public final void tell(Object message) {
        tell(message, noSender);
    }

    /**
     * 保留原 sender 的 forward 操作。
     *
     * @param message 待转发的消息
     * @param context 当前 ActorContext
     */
    public void forward(Object message, ActorContext context) {
        tell(message, context.sender());
    }

    @Override
    public final int compareTo(ActorRef other) {
        int comparison = path().compareTo(other.path());
        return comparison == 0 ? Integer.compare(path().uid(), other.path().uid()) : comparison;
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
