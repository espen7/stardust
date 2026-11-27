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

import io.github.espen7.tapio.TapioException;
import io.github.espen7.tapio.util.NoStackTrace;

import java.time.Duration;

/**
 * 当 {@link CircuitBreaker} 处于 {@code Open} 状态（或 {@code HalfOpen} 已有探测调用在进行）
 * 时快速失败抛出的异常，对应 Pekko {@code CircuitBreakerOpenException}。
 */
public class CircuitBreakerOpenException extends TapioException implements NoStackTrace {
    private final Duration remainingDuration;

    /**
     * 创建 CircuitBreakerOpenException。
     *
     * @param remainingDuration 距离下次尝试进入 HalfOpen 的剩余时长
     * @param message 错误消息
     */
    public CircuitBreakerOpenException(Duration remainingDuration, String message) {
        super(message);
        this.remainingDuration = remainingDuration;
    }

    /**
     * 创建使用默认消息的 CircuitBreakerOpenException。
     *
     * @param remainingDuration 距离下次尝试进入 HalfOpen 的剩余时长
     */
    public CircuitBreakerOpenException(Duration remainingDuration) {
        this(remainingDuration, "Circuit Breaker is open; calls are failing fast");
    }

    /**
     * 返回距离熔断器尝试半开（HalfOpen）的剩余时长。
     *
     * @return 剩余时长
     */
    public Duration remainingDuration() {
        return remainingDuration;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
