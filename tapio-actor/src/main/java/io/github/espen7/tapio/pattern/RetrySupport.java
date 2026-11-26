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

import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.util.NonFatal;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * 异步重试与指数退避延迟计算工具，对应 Pekko {@code RetrySupport}。
 */
public final class RetrySupport {
    private RetrySupport() {
    }

    /**
     * 立即重试异步操作，最多在初次尝试后再重试 {@code attempts} 次。
     */
    public static <T> CompletionStage<T> retry(Supplier<CompletionStage<T>> attempt, int attempts, Executor executor) {
        return retry(attempt, (value, error) -> error != null, attempts, executor);
    }

    /**
     * 使用自定义重试条件立即重试异步操作。
     */
    public static <T> CompletionStage<T> retry(
            Supplier<CompletionStage<T>> attempt,
            BiPredicate<T, Throwable> shouldRetry,
            int attempts,
            Executor executor) {
        return retry(attempt, shouldRetry, attempts, n -> Optional.empty(), executor, null);
    }

    /**
     * 按固定延迟重试异步操作。
     */
    public static <T> CompletionStage<T> retry(
            Supplier<CompletionStage<T>> attempt,
            int attempts,
            Duration delay,
            Executor executor,
            Scheduler scheduler) {
        Objects.requireNonNull(delay, "delay must not be null");
        return retry(attempt, (value, error) -> error != null, attempts,
                n -> Optional.of(delay), executor, Objects.requireNonNull(scheduler, "scheduler must not be null"));
    }

    /**
     * 按指数退避（含随机抖动）重试异步操作。
     */
    public static <T> CompletionStage<T> retry(
            Supplier<CompletionStage<T>> attempt,
            int attempts,
            Duration minBackoff,
            Duration maxBackoff,
            double randomFactor,
            Executor executor,
            Scheduler scheduler) {
        return retry(attempt, (value, error) -> error != null, attempts,
                n -> Optional.of(calculateDelay(n - 1, minBackoff, maxBackoff, randomFactor)),
                executor, Objects.requireNonNull(scheduler, "scheduler must not be null"));
    }

    /**
     * 使用自定义重试谓词与自定义延迟函数重试异步操作。
     */
    public static <T> CompletionStage<T> retry(
            Supplier<CompletionStage<T>> attempt,
            BiPredicate<T, Throwable> shouldRetry,
            int attempts,
            IntFunction<Optional<Duration>> delayFunction,
            Executor executor,
            Scheduler scheduler) {
        Objects.requireNonNull(attempt, "attempt must not be null");
        Objects.requireNonNull(shouldRetry, "shouldRetry must not be null");
        Objects.requireNonNull(delayFunction, "delayFunction must not be null");
        Objects.requireNonNull(executor, "executor must not be null");
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must be >= 0");
        }
        return retryInternal(attempt, shouldRetry, attempts, delayFunction, 0, executor, scheduler);
    }

    private static <T> CompletionStage<T> retryInternal(
            Supplier<CompletionStage<T>> attempt,
            BiPredicate<T, Throwable> shouldRetry,
            int attempts,
            IntFunction<Optional<Duration>> delayFunction,
            int attempted,
            Executor executor,
            Scheduler scheduler) {
        CompletionStage<T> current;
        try {
            current = attempt.get();
        } catch (Throwable error) {
            if (!NonFatal.apply(error)) {
                throw error;
            }
            current = CompletableFuture.failedFuture(error);
        }
        if (attempted >= attempts) {
            return current;
        }
        return current.handleAsync((value, error) -> {
            if (!shouldRetry.test(value, error)) {
                return error != null
                        ? CompletableFuture.<T>failedFuture(error)
                        : CompletableFuture.completedFuture(value);
            }
            Optional<Duration> delay = delayFunction.apply(attempted + 1);
            if (delay.isEmpty() || delay.get().isZero() || delay.get().isNegative()) {
                return retryInternal(attempt, shouldRetry, attempts, delayFunction, attempted + 1, executor, scheduler);
            }
            CompletableFuture<T> next = new CompletableFuture<>();
            scheduler.scheduleOnce(delay.get(), () ->
                    retryInternal(attempt, shouldRetry, attempts, delayFunction, attempted + 1, executor, scheduler)
                            .whenComplete((v, e) -> {
                                if (e != null) {
                                    next.completeExceptionally(e);
                                } else {
                                    next.complete(v);
                                }
                            }), executor);
            return next;
        }, executor).thenCompose(stage -> stage);
    }

    /**
     * 计算第 {@code restartCount} 次重启的指数退避延迟（受 {@code maxBackoff} 上限约束并附加随机抖动）。
     *
     * @param restartCount 当前重启次数
     * @param minBackoff 最小退避时长
     * @param maxBackoff 最大退避时长
     * @param randomFactor 随机抖动因子
     * @return 计算得到的退避时长
     */
    public static Duration calculateDelay(int restartCount, Duration minBackoff, Duration maxBackoff, double randomFactor) {
        double rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor;
        double baseNanos = minBackoff.toNanos() * Math.pow(2.0, restartCount);
        double maxNanos = maxBackoff.toNanos();
        double cappedNanos = Math.min(maxNanos, baseNanos) * rnd;
        if (!Double.isFinite(cappedNanos) || cappedNanos >= Long.MAX_VALUE) {
            return maxBackoff;
        }
        return Duration.ofNanos((long) cappedNanos);
    }
}
