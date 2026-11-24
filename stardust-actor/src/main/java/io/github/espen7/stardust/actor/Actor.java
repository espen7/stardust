package io.github.espen7.stardust.actor;

/**
 * Actor 的生命周期和消息接收协议。
 *
 * <p>ActorCell 在 actor creation、message processing、restart 和 termination 的对应阶段
 * 调用这些方法。应用代码通常继承 {@link AbstractActor}，并实现
 * {@link AbstractActor#createReceive()}。</p>
 */
public interface Actor {
    /**
     * 返回此 Actor 用于处理 child failure 的监督策略。
     *
     * @return supervisor strategy
     */
    default SupervisorStrategy supervisorStrategy() {
        return SupervisorStrategy.defaultStrategy;
    }

    /** Actor 实例创建后、开始处理用户消息前调用。 */
    void preStart() throws Exception;

    /** Actor 终止时调用，用于释放实例持有的资源。 */
    void postStop() throws Exception;

    /**
     * 旧实例 restart 前调用。
     *
     * @param reason 触发 restart 的异常
     * @param message 触发失败的消息；初始化失败时为 {@code null}
     */
    void preRestart(Throwable reason, Object message) throws Exception;

    /** 新实例创建后调用，默认由 {@link AbstractActor} 转调 {@link #preStart()}。 */
    void postRestart(Throwable reason) throws Exception;

    /** Receive 没有处理指定消息时调用。 */
    void unhandled(Object message);

    /** 返回此 Actor 的初始 receive behavior。 */
    AbstractActor.Receive receive();
}
