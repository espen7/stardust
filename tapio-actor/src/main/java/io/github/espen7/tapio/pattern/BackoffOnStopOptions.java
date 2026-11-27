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

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.OneForOneStrategy;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.SupervisorStrategy;
import io.github.espen7.tapio.pattern.internal.BackoffOnStopSupervisor;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * 配置按子 Actor 停止（Terminated）触发指数退避的 {@link BackoffSupervisor} 选项
 * {@code BackoffOnStopOptions}。
 */
public final class BackoffOnStopOptions {
    private final Props childProps;
    private final String childName;
    private final Duration minBackoff;
    private final Duration maxBackoff;
    private final double randomFactor;
    private final BackoffReset reset;
    private final SupervisorStrategy supervisorStrategy;
    private final HandlingWhileStopped handlingWhileStopped;
    private final Predicate<Object> finalStopMessage;

    BackoffOnStopOptions(Props childProps, String childName, Duration minBackoff, Duration maxBackoff,
                         double randomFactor, BackoffReset reset, SupervisorStrategy supervisorStrategy,
                         HandlingWhileStopped handlingWhileStopped, Predicate<Object> finalStopMessage) {
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
        this.finalStopMessage = finalStopMessage;
    }

    /** 设置稳定运行多久后自动重置退避计数。 */
    public BackoffOnStopOptions withAutoReset(Duration resetBackoff) {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                new BackoffReset.AutoReset(resetBackoff), supervisorStrategy, handlingWhileStopped, finalStopMessage);
    }

    /** 设置为仅通过 {@link BackoffSupervisor#reset()} 手动重置退避计数。 */
    public BackoffOnStopOptions withManualReset() {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                BackoffReset.ManualReset.INSTANCE, supervisorStrategy, handlingWhileStopped, finalStopMessage);
    }

    /** 设置监督策略（可通过 {@link OneForOneStrategy#withMaxNrOfRetries(int)} 限制最大重启次数）。 */
    public BackoffOnStopOptions withSupervisorStrategy(SupervisorStrategy supervisorStrategy) {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, Objects.requireNonNull(supervisorStrategy), handlingWhileStopped, finalStopMessage);
    }

    /** 设置使用默认停止监督策略。 */
    public BackoffOnStopOptions withDefaultStoppingStrategy() {
        return withSupervisorStrategy(SupervisorStrategy.stoppingStrategy);
    }

    /** 设置子 Actor 退避停止期间向发送者回复的固定消息。 */
    public BackoffOnStopOptions withReplyWhileStopped(Object replyWhileStopped) {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, supervisorStrategy, new HandlingWhileStopped.ReplyWith(replyWhileStopped), finalStopMessage);
    }

    /** 设置子 Actor 退避停止期间转发消息的目标 Actor。 */
    public BackoffOnStopOptions withHandlerWhileStopped(ActorRef handler) {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, supervisorStrategy, new HandlingWhileStopped.ForwardTo(handler), finalStopMessage);
    }

    /** 设置触发最终停止（不再重启）的消息谓词。 */
    public BackoffOnStopOptions withFinalStopMessage(Predicate<Object> isFinalStopMessage) {
        return new BackoffOnStopOptions(childProps, childName, minBackoff, maxBackoff, randomFactor,
                reset, supervisorStrategy, handlingWhileStopped, isFinalStopMessage);
    }

    Props props() {
        return Props.create(BackoffOnStopSupervisor.class, childProps, childName, minBackoff, maxBackoff,
                reset, randomFactor, supervisorStrategy, handlingWhileStopped, finalStopMessage);
    }
}
