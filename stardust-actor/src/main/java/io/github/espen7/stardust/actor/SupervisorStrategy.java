package io.github.espen7.stardust.actor;

import java.time.Duration;
import java.util.function.Function;

/**
 * 将 child failure 映射为监督指令。
 *
 * <p>Resume 保留当前实例，Restart 重建实例，Stop 终止 child，Escalate 将失败
 * 交给更上层 supervisor。defaultDecider 对普通 Exception 执行 Restart。</p>
 */
public abstract class SupervisorStrategy {
    /**
     * 监督器对 child failure 作出的决定。
     */
    public enum Directive {Resume, Restart, Stop, Escalate}

    /**
     * Pekko 默认决策：初始化失败停止，普通异常重启。
     */
    public static final Function<Throwable, Directive> defaultDecider = error ->
            error instanceof ActorInitializationException ? Directive.Stop : Directive.Restart;
    public static final SupervisorStrategy defaultStrategy = new OneForOneStrategy(-1, Duration.ZERO, defaultDecider);

    /**
     * 为失败计算监督指令。
     */
    public abstract Directive handle(Throwable cause);
}
