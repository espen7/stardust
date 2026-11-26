/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.actor;

import io.github.espen7.tapio.actor.dungeon.ChildRestartStats;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

/**
 * 仅对失败 child 应用监督指令的策略。
 */
public class OneForOneStrategy extends SupervisorStrategy {
    private final int maxNrOfRetries;
    private final Duration withinTimeRange;
    private final Function<Throwable, Directive> decider;
    private final boolean loggingEnabled;

    /**
     * 创建启用失败日志的 OneForOneStrategy。
     *
     * @param maxNrOfRetries 时间窗内允许的最大重试数，负数表示不限制重试数
     * @param withinTimeRange 重试计数时间窗，负 duration 表示不使用时间窗
     * @param decider 将失败异常映射为监督指令的函数
     */
    public OneForOneStrategy(int maxNrOfRetries, Duration withinTimeRange,
                             Function<Throwable, Directive> decider) {
        this(maxNrOfRetries, withinTimeRange, decider, true);
    }

    /**
     * 创建对指定异常类型执行 Restart、其他异常向上升级的策略。
     *
     * @param maxNrOfRetries 时间窗内允许的最大重试数，负数表示不限制重试数
     * @param withinTimeRange 重试计数时间窗，负 duration 表示不使用时间窗
     * @param trapExit 需要重启的异常类型
     */
    public OneForOneStrategy(int maxNrOfRetries, Duration withinTimeRange,
                             Iterable<Class<? extends Throwable>> trapExit) {
        this(maxNrOfRetries, withinTimeRange, SupervisorStrategy.makeDecider(trapExit));
    }

    /**
     * 创建 OneForOneStrategy。
     *
     * @param maxNrOfRetries 时间窗内允许的最大重试数，负数表示不限制重试数
     * @param withinTimeRange 重试计数时间窗，负 duration 表示不使用时间窗
     * @param decider 将失败异常映射为监督指令的函数
     * @param loggingEnabled 是否通过 EventStream 发布失败日志
     */
    public OneForOneStrategy(int maxNrOfRetries, Duration withinTimeRange,
                             Function<Throwable, Directive> decider, boolean loggingEnabled) {
        this.maxNrOfRetries = maxNrOfRetries;
        this.withinTimeRange = Objects.requireNonNull(withinTimeRange, "withinTimeRange");
        this.decider = Objects.requireNonNull(decider, "decider");
        this.loggingEnabled = loggingEnabled;
    }

    /**
     * 创建不限制重试次数和重试时间窗的策略。
     *
     * @param loggingEnabled 是否通过 EventStream 发布失败日志
     * @param decider 将失败异常映射为监督指令的函数
     */
    public OneForOneStrategy(boolean loggingEnabled, Function<Throwable, Directive> decider) {
        this(-1, Duration.ofNanos(-1), decider, loggingEnabled);
    }

    /**
     * 创建不限制重试次数和重试时间窗、启用失败日志的策略。
     *
     * @param decider 将失败异常映射为监督指令的函数
     */
    public OneForOneStrategy(Function<Throwable, Directive> decider) {
        this(-1, Duration.ofNanos(-1), decider);
    }

    /**
     * 返回时间窗内允许的最大重试数。
     *
     * @return 配置的重试数，负数表示不限制
     */
    public int maxNrOfRetries() {
        return maxNrOfRetries;
    }

    /**
     * 返回重试计数时间窗。
     *
     * @return 配置的时间窗，负 duration 表示无时间窗
     */
    public Duration withinTimeRange() {
        return withinTimeRange;
    }

    /**
     * 返回仅调整最大重试次数的新策略。
     *
     * @param maxNrOfRetries 时间窗内允许的最大重试数，负数表示不限制重试数
     * @return 保留时间窗、decider 和日志设置的新策略
     */
    public OneForOneStrategy withMaxNrOfRetries(int maxNrOfRetries) {
        return new OneForOneStrategy(maxNrOfRetries, withinTimeRange, decider, loggingEnabled);
    }

    /**
     * 返回此策略使用的 decider。
     *
     * @return 监督决策函数
     */
    @Override
    public Function<Throwable, Directive> decider() {
        return decider;
    }

    /**
     * 根据失败原因计算监督指令。
     *
     * @param cause child failure 原因
     * @return decider 产生的监督指令
     */
    @Override
    public Directive handle(Throwable cause) {
        return decider.apply(cause);
    }

    /**
     * 返回失败日志是否启用。
     *
     * @return 启用时为 {@code true}
     */
    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    /**
     * 按 One-for-One 规则处理 child failure。
     *
     * @param context 当前 supervisor 的 ActorContext
     * @param restart 是否执行 restart
     * @param child 失败的 child
     * @param cause 失败原因
     * @param stats 失败 child 的重启统计
     * @param children 当前所有 child 的重启统计
     */
    @Override
    public void processFailure(ActorContext context, boolean restart, ActorRef child, Throwable cause,
                               ChildRestartStats stats, Collection<ChildRestartStats> children) {
        if (restart && stats.requestRestartPermission(maxNrOfRetries, windowMillis(withinTimeRange))) {
            restartChild(child, cause, false);
        } else {
            context.stop(child);
        }
    }

}
