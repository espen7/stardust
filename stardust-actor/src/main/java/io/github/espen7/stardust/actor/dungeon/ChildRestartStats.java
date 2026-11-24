package io.github.espen7.stardust.actor.dungeon;

import io.github.espen7.stardust.actor.ActorRef;

/**
 * 保存 child 引用及其重启统计信息。当前阶段保留容器所需的 child 身份。
 */
public record ChildRestartStats(ActorRef child) implements ChildStats {
}
