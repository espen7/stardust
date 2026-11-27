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
import io.github.espen7.tapio.pattern.internal.BackoffOnRestartSupervisor;

import java.time.Duration;
import java.util.Objects;

/**
 * 配置按失败（Restart 指令）触发指数退避的 {@link BackoffSupervisor} 选项
 * {@code BackoffOnFailureOptions}。
 */
public final class BackoffOnFailureOptions {
    private final Props childProps;
    private final String childName;
    private final Duration minBackoff;
    private final Duration maxBackoff;
    private final double randomFactor;
    private final BackoffReset reset;
    private final OneForOneStrategy supervisorStrategy;
    private final HandlingWhileStopped handlingWhileStopped;

    BackoffOnFailureOptions(Props childProps, String childName, Duration minBackoff, Duration maxBackoff,
                            double randomFactor, BackoffReset reset, OneForOneStrategy supervisorStrategy,
                            HandlingWhileStopped handlingWhileStopped) {
        if (minBackoff == null || minBackoff.isNegative() || minBackoff.isZero()) {
            throw new IllegalArgumentException("minBackoff must be > 0");
        }
        if (maxBackoff == null || maxBackoff.compareTo(minBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must be >= minBackoff");
        }
        if (randomFactor < 0.0 || randomFactor > 1.0) {
            throw new IllegalArgumentException("randomFactor must be between 0.0 and 1.0");
        }
        this.childProps = Objects.requireNonNull(childProps, "childProps must not be null");
        this.childName = Objects.requireNonNull(childName, "childName must not be null");
        this.minBackoff = minBackoff;
        this.maxBackoff = maxBackoff;
        this.randomFactor = randomFactor;
        this.reset = reset;
        this.supervisorStrategy = supervisorStrategy;
        this.handlingWhileStopped = handlingWhileStopped;
    }

    /** 设置稳定运行多久后自动重置退避计数。 */
    public BackoffOnFailureOptions withAutoReset(Duration resetBackoff) {
        return new BackoffOnFailureOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                new BackoffReset.AutoReset(resetBackoff), supervisorStrategy, handlingWhileStopped);
    }

    /** 设置为仅通过 {@link BackoffSupervisor#reset()} 手动重置退避计数。 */
    public BackoffOnFailureOptions withManualReset() {
        return new BackoffOnFailureOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                BackoffReset.ManualReset.INSTANCE, supervisorStrategy, handlingWhileStopped);
    }

    /** 设置用于决定子 Actor 异常指令的 {@link OneForOneStrategy}。 */
    public BackoffOnFailureOptions withSupervisorStrategy(OneForOneStrategy supervisorStrategy) {
        return new BackoffOnFailureOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, Objects.requireNonNull(supervisorStrategy), handlingWhileStopped);
    }

    /** 设置子 Actor 退避停止期间向发送者回复的固定消息。 */
    public BackoffOnFailureOptions withReplyWhileStopped(Object replyWhileStopped) {
        return new BackoffOnFailureOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, supervisorStrategy, new HandlingWhileStopped.ReplyWith(replyWhileStopped));
    }

    /** 设置子 Actor 退避停止期间转发消息的目标 Actor。 */
    public BackoffOnFailureOptions withHandlerWhileStopped(io.github.espen7.tapio.actor.ActorRef handler) {
        return new BackoffOnFailureOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, supervisorStrategy, new HandlingWhileStopped.ForwardTo(handler));
    }

    Props props() {
        if (reset instanceof BackoffReset.AutoReset(Duration resetBackoff)) {
            if (minBackoff.compareTo(resetBackoff) > 0 || resetBackoff.compareTo(maxBackoff) > 0) {
                throw new IllegalArgumentException(
                        "minBackoff must be <= resetBackoff <= maxBackoff");
            }
        }
        return Props.create(BackoffOnRestartSupervisor.class, childProps, childName, minBackoff, maxBackoff,
                reset, randomFactor, supervisorStrategy, handlingWhileStopped);
    }
}
