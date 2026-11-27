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
package io.github.espen7.tapio.pattern;

import io.github.espen7.tapio.actor.OneForOneStrategy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.SupervisorStrategy;

import java.time.Duration;

/**
 * 构造 {@link BackoffSupervisor} 退避选项的工厂入口。
 */
public final class BackoffOpts {
    private BackoffOpts() {
    }

    /**
     * 创建基于失败（Restart 指令）触发指数退避重启的监督选项。
     *
     * @param childProps 子 Actor Props
     * @param childName 子 Actor 名称
     * @param minBackoff 最小退避时长（必须大于 0）
     * @param maxBackoff 最大退避时长（必须大于等于 {@code minBackoff}）
     * @param randomFactor 随机抖动因子（0.0 到 1.0）
     * @return BackoffOnFailureOptions
     */
    public static BackoffOnFailureOptions onFailure(
            Props childProps, String childName, Duration minBackoff, Duration maxBackoff, double randomFactor) {
        return new BackoffOnFailureOptions(
                childProps, childName, minBackoff, maxBackoff, randomFactor,
                new BackoffReset.AutoReset(minBackoff),
                (OneForOneStrategy) SupervisorStrategy.defaultStrategy,
                HandlingWhileStopped.ForwardDeathLetters.INSTANCE);
    }

    /**
     * 创建基于子 Actor 停止（Terminated）触发指数退避重启的监督选项。
     *
     * @param childProps 子 Actor Props
     * @param childName 子 Actor 名称
     * @param minBackoff 最小退避时长（必须大于 0）
     * @param maxBackoff 最大退避时长（必须大于等于 {@code minBackoff}）
     * @param randomFactor 随机抖动因子（0.0 到 1.0）
     * @return BackoffOnStopOptions
     */
    public static BackoffOnStopOptions onStop(
            Props childProps, String childName, Duration minBackoff, Duration maxBackoff, double randomFactor) {
        return new BackoffOnStopOptions(
                childProps, childName, minBackoff, maxBackoff, randomFactor,
                new BackoffReset.AutoReset(minBackoff),
                SupervisorStrategy.defaultStrategy,
                HandlingWhileStopped.ForwardDeathLetters.INSTANCE,
                null);
    }
}
