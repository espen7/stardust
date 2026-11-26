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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * 提供 {@code Closed} / {@code Open} / {@code HalfOpen} 三态保护的熔断器实现，对应 Pekko
 * {@code CircuitBreaker}。
 */
public class CircuitBreaker {
    private sealed interface State permits Closed, Open, HalfOpen {
        void invoke(CircuitBreaker cb);
    }

    private final class Closed implements State {
        private final AtomicInteger failures = new AtomicInteger(0);
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(CircuitBreaker cb) {
            cb.notifyTransitionListeners(listeners);
        }

        int currentFailureCount() {
            return failures.get();
        }

        void onSuccess() {
            failures.set(0);
        }

        void onFailure() {
            if (failures.incrementAndGet() >= maxFailures && currentState.compareAndSet(this, new Open())) {
                currentState.get().invoke(CircuitBreaker.this);
            }
        }
    }

    private final class Open implements State {
        private final long startedAtNanos = System.nanoTime();
        private final Duration effectiveResetTimeout = nextResetTimeout();
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(CircuitBreaker cb) {
            cb.notifyTransitionListeners(listeners);
            scheduler.scheduleOnce(effectiveResetTimeout, () -> {
                if (currentState.compareAndSet(this, new HalfOpen())) {
                    currentState.get().invoke(CircuitBreaker.this);
                }
            }, executor);
        }

        Duration remainingDuration() {
            long elapsedNanos = System.nanoTime() - startedAtNanos;
            long remaining = Math.max(0L, effectiveResetTimeout.toNanos() - elapsedNanos);
            return Duration.ofNanos(remaining);
        }

        private Duration nextResetTimeout() {
            int count = openCount.getAndIncrement();
            if (exponentialBackoffFactor <= 1.0 && randomFactor == 0.0) {
                return resetTimeout;
            }
            double multiplier = Math.pow(exponentialBackoffFactor, count);
            double baseNanos = resetTimeout.toNanos() * multiplier;
            double maxNanos = maxResetTimeout.toNanos();
            double cappedNanos = Math.min(maxNanos, baseNanos);
            return Duration.ofNanos((long) cappedNanos);
        }
    }

    private final class HalfOpen implements State {
        private final AtomicBoolean lock = new AtomicBoolean(true);
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void invoke(CircuitBreaker cb) {
            cb.notifyTransitionListeners(listeners);
        }

        boolean tryAcquire() {
            return lock.compareAndSet(true, false);
        }

        void onSuccess() {
            openCount.set(0);
            if (currentState.compareAndSet(this, new Closed())) {
                currentState.get().invoke(CircuitBreaker.this);
            }
        }

        void onFailure() {
            if (currentState.compareAndSet(this, new Open())) {
                currentState.get().invoke(CircuitBreaker.this);
            }
        }
    }

    private final Scheduler scheduler;
    private final int maxFailures;
    private final Duration callTimeout;
    private final Duration resetTimeout;
    private final Duration maxResetTimeout;
    private final double exponentialBackoffFactor;
    private final double randomFactor;
    private final Executor executor;

    private final AtomicInteger openCount = new AtomicInteger(0);
    private final AtomicReference<State> currentState = new AtomicReference<>();
    private final CopyOnWriteArrayList<Runnable> onOpenListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> onHalfOpenListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> onCloseListeners = new CopyOnWriteArrayList<>();

    private CircuitBreaker(Scheduler scheduler, int maxFailures, Duration callTimeout, Duration resetTimeout,
                           Duration maxResetTimeout, double exponentialBackoffFactor, double randomFactor,
                           Executor executor) {
        if (maxFailures < 0) {
            throw new IllegalArgumentException("maxFailures must be >= 0");
        }
        if (callTimeout == null || callTimeout.isNegative()) {
            throw new IllegalArgumentException("callTimeout must be >= 0");
        }
        if (resetTimeout == null || resetTimeout.isNegative()) {
            throw new IllegalArgumentException("resetTimeout must be >= 0");
        }
        if (exponentialBackoffFactor < 1.0) {
            throw new IllegalArgumentException("factor must be >= 1.0");
        }
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.maxFailures = maxFailures;
        this.callTimeout = callTimeout;
        this.resetTimeout = resetTimeout;
        this.maxResetTimeout = maxResetTimeout;
        this.exponentialBackoffFactor = exponentialBackoffFactor;
        this.randomFactor = randomFactor;
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.currentState.set(new Closed());
    }

    /**
     * 创建默认在调用线程/同步执行器上运行回调的 {@link CircuitBreaker}。
     */
    public static CircuitBreaker create(Scheduler scheduler, int maxFailures, Duration callTimeout,
                                        Duration resetTimeout) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Duration.ofDays(36500), 1.0, 0.0, Runnable::run);
    }

    /**
     * 使用指定异步回调执行器创建 {@link CircuitBreaker}。
     */
    public static CircuitBreaker create(Scheduler scheduler, int maxFailures, Duration callTimeout,
                                        Duration resetTimeout, Executor executor) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Duration.ofDays(36500), 1.0, 0.0, executor);
    }

    /** 配置指数退避上限。 */
    public CircuitBreaker withMaxResetTimeout(Duration maxResetTimeout) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Objects.requireNonNull(maxResetTimeout), exponentialBackoffFactor, randomFactor, executor);
    }

    /** 配置指数退避因子（需 {@code >= 1.0}）。 */
    public CircuitBreaker withExponentialBackoff(Duration maxResetTimeout) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Objects.requireNonNull(maxResetTimeout), 2.0, randomFactor, executor);
    }

    /** 注册进入 {@code Open} 状态时的回调。 */
    public CircuitBreaker onOpen(Runnable callback) {
        onOpenListeners.add(Objects.requireNonNull(callback));
        return this;
    }

    /** 注册进入 {@code HalfOpen} 状态时的回调。 */
    public CircuitBreaker onHalfOpen(Runnable callback) {
        onHalfOpenListeners.add(Objects.requireNonNull(callback));
        return this;
    }

    /** 注册回到 {@code Closed} 状态时的回调。 */
    public CircuitBreaker onClose(Runnable callback) {
        onCloseListeners.add(Objects.requireNonNull(callback));
        return this;
    }

    /** 当前是否处于 {@code Closed} 状态。 */
    public boolean isClosed() {
        return currentState.get() instanceof Closed;
    }

    /** 当前是否处于 {@code Open} 状态。 */
    public boolean isOpen() {
        return currentState.get() instanceof Open;
    }

    /** 当前是否处于 {@code HalfOpen} 状态。 */
    public boolean isHalfOpen() {
        return currentState.get() instanceof HalfOpen;
    }

    /** 返回当前连续失败计数。 */
    public long currentFailureCount() {
        return currentState.get() instanceof Closed closed ? closed.currentFailureCount() : 0L;
    }

    /** 手动记录一次成功。 */
    public void succeed() {
        callSucceeds();
    }

    /** 手动记录一次失败。 */
    public void fail() {
        callFails();
    }

    /**
     * 通过熔断器保护异步调用。
     *
     * @param body 异步调用提供者
     * @param <T> 返回值类型
     * @return 受熔断与超时保护的 {@link CompletionStage}
     */
    public <T> CompletionStage<T> callWithCircuitBreaker(Supplier<CompletionStage<T>> body) {
        return callWithCircuitBreaker(body, (value, error) -> error != null);
    }

    /**
     * 通过熔断器保护异步调用，并使用自定义失败判定器。
     *
     * @param body 异步调用提供者
     * @param defineFailure 判断返回值/异常是否视为失败的谓词
     * @param <T> 返回值类型
     * @return 受熔断与超时保护的 {@link CompletionStage}
     */
    public <T> CompletionStage<T> callWithCircuitBreaker(Supplier<CompletionStage<T>> body,
                                                         BiFunction<T, Throwable, Boolean> defineFailure) {
        State state = currentState.get();
        return switch (state) {
            case Closed closed -> executeAsync(body, defineFailure);
            case Open open -> CompletableFuture.failedFuture(new CircuitBreakerOpenException(open.remainingDuration()));
            case HalfOpen halfOpen -> {
                if (halfOpen.tryAcquire()) {
                    yield executeAsync(body, defineFailure);
                }
                yield CompletableFuture.failedFuture(new CircuitBreakerOpenException(Duration.ZERO));
            }
        };
    }

    /**
     * 通过熔断器保护同步调用。
     *
     * @param body 同步调用
     * @param <T> 返回值类型
     * @return 调用结果
     * @throws Exception 调用失败、超时或熔断器开启时抛出
     */
    public <T> T callWithSyncCircuitBreaker(Callable<T> body) throws Exception {
        State state = currentState.get();
        return switch (state) {
            case Closed closed -> executeSync(body);
            case Open open -> throw new CircuitBreakerOpenException(open.remainingDuration());
            case HalfOpen halfOpen -> {
                if (halfOpen.tryAcquire()) {
                    yield executeSync(body);
                }
                throw new CircuitBreakerOpenException(Duration.ZERO);
            }
        };
    }

    private <T> CompletionStage<T> executeAsync(Supplier<CompletionStage<T>> body,
                                                BiFunction<T, Throwable, Boolean> defineFailure) {
        long startNanos = System.nanoTime();
        CompletionStage<T> stage;
        try {
            stage = body.get();
        } catch (Throwable error) {
            callFails();
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        var timeoutTask = scheduler.scheduleOnce(callTimeout, () ->
                result.completeExceptionally(new TimeoutException("Call exceeded callTimeout of " + callTimeout)), executor);
        stage.whenCompleteAsync((value, error) -> {
            timeoutTask.cancel();
            if (error != null) {
                result.completeExceptionally(error);
            } else {
                result.complete(value);
            }
        }, executor);
        return result.whenComplete((value, error) -> {
            long elapsedNanos = System.nanoTime() - startNanos;
            boolean failed = Boolean.TRUE.equals(defineFailure.apply(value, error))
                    || elapsedNanos >= callTimeout.toNanos();
            if (failed) {
                callFails();
            } else {
                callSucceeds();
            }
        });
    }

    private <T> T executeSync(Callable<T> body) throws Exception {
        long startNanos = System.nanoTime();
        try {
            T result = body.call();
            long elapsedNanos = System.nanoTime() - startNanos;
            if (elapsedNanos >= callTimeout.toNanos()) {
                callFails();
            } else {
                callSucceeds();
            }
            return result;
        } catch (Throwable error) {
            callFails();
            if (error instanceof Exception ex) {
                throw ex;
            }
            if (error instanceof Error fatal) {
                throw fatal;
            }
            throw new RuntimeException(error);
        }
    }

    private void callSucceeds() {
        State state = currentState.get();
        if (state instanceof Closed closed) {
            closed.onSuccess();
        } else if (state instanceof HalfOpen halfOpen) {
            halfOpen.onSuccess();
        }
    }

    private void callFails() {
        State state = currentState.get();
        if (state instanceof Closed closed) {
            closed.onFailure();
        } else if (state instanceof HalfOpen halfOpen) {
            halfOpen.onFailure();
        }
    }

    private void notifyTransitionListeners(CopyOnWriteArrayList<Runnable> stateListeners) {
        State state = currentState.get();
        CopyOnWriteArrayList<Runnable> list = switch (state) {
            case Open ignored -> onOpenListeners;
            case HalfOpen ignored -> onHalfOpenListeners;
            case Closed ignored -> onCloseListeners;
        };
        for (Runnable listener : list) {
            executor.execute(listener);
        }
        for (Runnable listener : stateListeners) {
            executor.execute(listener);
        }
    }
}
