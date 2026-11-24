package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorCell;

/**
 * Pekko dungeon.FaultHandling 的 Java 对照入口。
 *
 * <p>失败处理由 ActorCell 的当前 supervisor strategy 决定。restart 和 terminate
 * 都作为系统消息排队，避免在 receive 回调尚未退出时重入生命周期钩子。</p>
 */
public final class FaultHandling {
    private FaultHandling() {
    }

    /**
     * 请求 ActorCell 重新创建 Actor 实例。
     */
    public static void restart(ActorCell cell, Throwable cause) {
        cell.sendSystemMessage(new io.github.espen7.stardust.dispatch.sysmsg.SystemMessage.Recreate(cause));
    }

    /**
     * 请求 ActorCell 完成终止流程。
     */
    public static void terminate(ActorCell cell) {
        cell.sendSystemMessage(new io.github.espen7.stardust.dispatch.sysmsg.SystemMessage.Terminate());
    }
}
