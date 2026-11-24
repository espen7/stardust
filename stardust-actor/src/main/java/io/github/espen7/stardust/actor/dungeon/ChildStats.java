package io.github.espen7.stardust.actor.dungeon;

/**
 * ChildrenContainer 中的条目基类，保留 Pekko ChildStats 的容器形状。
 */
public sealed interface ChildStats permits ChildRestartStats, ChildNameReserved {
}
