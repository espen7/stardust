package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorRef;

/**
 * Pekko dungeon.DeathWatch 的 Java 对照入口。
 *
 * <p>watch 和 unwatch 都以 SystemMessage 发送给被观察对象，由对方的 ActorCell
 * 串行更新 watchedBy。观察已终止的对象时，运行时需要立即投递 Terminated。</p>
 */
public final class DeathWatch {
    private DeathWatch() {
    }

    /**
     * 开始观察 subject 的终止状态。
     */
    public static ActorRef watch(ActorCell cell, ActorRef subject) {
        return cell.watch(subject);
    }

    /**
     * 取消之前注册的观察关系。
     */
    public static ActorRef unwatch(ActorCell cell, ActorRef subject) {
        return cell.unwatch(subject);
    }
}
