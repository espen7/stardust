package io.github.espen7.stardust.actor;

import java.io.Serializable;

/**
 * Classic Actor 用于回复操作结果的消息。
 *
 * @see io.github.espen7.stardust.pattern.Patterns#ask
 */
public sealed interface Status extends Serializable permits Status.Success, Status.Failure {
    /**
     * 表示操作成功。ask 解包此消息并以结果值完成。
     *
     * @param status 操作结果，可以为 {@code null}
     */
    record Success(Object status) implements Status {}

    /**
     * 表示操作失败。ask 以提供的原因失败。
     *
     * @param cause 操作失败的原因，须为非 {@code null}
     */
    record Failure(Throwable cause) implements Status {}
}
