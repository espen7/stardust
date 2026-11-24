package io.github.espen7.stardust.actor;

/**
 * 消息不满足 Actor 投递约束时抛出的异常。
 */
public final class InvalidMessageException extends RuntimeException {
    /**
     * 创建消息投递异常。
     *
     * @param message 描述违反的投递约束
     */
    public InvalidMessageException(String message) {
        super(message);
    }
}
