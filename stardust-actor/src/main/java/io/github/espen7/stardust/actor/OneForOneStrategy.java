package io.github.espen7.stardust.actor;

import java.time.Duration;
import java.util.function.Function;

/**
 * 每次只对失败的 child 应用监督指令，对应 Pekko OneForOneStrategy。
 */
public class OneForOneStrategy extends SupervisorStrategy {

    private final int maxNrOfRetries;
    private final Duration withinTimeRange;
    private final Function<Throwable, Directive> decider;

    /**
     * 创建 OneForOneStrategy。
     *
     * <p>maxNrOfRetries 和 withinTimeRange 保留 Pekko 的 API 概念。当前版本只保存
     * 参数，重试窗口的计数和限制逻辑尚未移植。</p>
     */
    public OneForOneStrategy(int maxNrOfRetries, Duration withinTimeRange, Function<Throwable, Directive> decider) {
        this.maxNrOfRetries = maxNrOfRetries;
        this.withinTimeRange = withinTimeRange;
        this.decider = decider;
    }

    public OneForOneStrategy(Function<Throwable, Directive> decider) {
        this(-1, Duration.ZERO, decider);
    }

    public int maxNrOfRetries() {
        return maxNrOfRetries;
    }

    public Duration withinTimeRange() {
        return withinTimeRange;
    }

    @Override
    public Directive handle(Throwable cause) {
        return decider.apply(cause);
    }
}
