package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * 请求 Actor 返回其引用。
 *
 * @param messageId 原样放入回复中的关联标识
 */
public record Identify(Object messageId) implements Serializable {
}
