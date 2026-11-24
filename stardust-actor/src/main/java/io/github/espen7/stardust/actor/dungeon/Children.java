package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorRef;
import io.github.espen7.stardust.actor.Props;

/**
 * Pekko dungeon.Children 的 Java 对照入口。
 *
 * <p>Scala trait 在 ActorCell 中通过混入共享状态和方法。Java 版本无法采用相同
 * 的 trait composition，因此把入口保留在同名类中，并委托给 ActorCell 管理状态。</p>
 */
public final class Children {
    private Children() {
    }

    /**
     * 通过 ActorCell 的 ChildrenContainer 预留 name 并创建 child。
     */
    public static ActorRef actorOf(ActorCell cell, Props props, String name) {
        return cell.actorOf(props, name);
    }

    /**
     * 通过 ActorRef 发起异步停止。
     */
    public static void stop(ActorCell cell, ActorRef ref) {
        cell.stop(ref);
    }
}
