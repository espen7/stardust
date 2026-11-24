package io.github.espen7.stardust.actor;

/**
 * Actor constructor 或初始 receive 创建失败时使用的异常。
 */
public class ActorInitializationException extends RuntimeException {
    public ActorInitializationException(Throwable cause) {
        super(cause);
    }

    public ActorInitializationException(String message) {
        super(message);
    }
}
