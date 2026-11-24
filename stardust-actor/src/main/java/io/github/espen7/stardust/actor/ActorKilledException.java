package io.github.espen7.stardust.actor;

/**
 * 表示 Actor 收到 Pekko Classic 的 Kill 指令后产生的异常。
 *
 * <p>默认监督策略对该异常执行 {@link SupervisorStrategy.Directive#Stop}。</p>
 */
public final class ActorKilledException extends RuntimeException {
    /**
     * 创建 ActorKilledException。
     *
     * @param message 异常消息
     */
    public ActorKilledException(String message) {
        super(message);
    }
}
