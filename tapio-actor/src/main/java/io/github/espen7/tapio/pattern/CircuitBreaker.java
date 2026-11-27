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
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.util.NonFatal;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * 提供 {@code Closed} / {@code Open} / {@code HalfOpen} 三态保护的熔断器实现。
 *
 * <p>状态对象为单例实例（非每次转换创建新实例）。</p>
 */
public class CircuitBreaker {

    @InternalApi
    sealed interface State permits CircuitBreaker.ClosedState, CircuitBreaker.OpenState, CircuitBreaker.HalfOpenState {
        void enter();
        void callSucceeds();
        void callFails();
    }

    private final Scheduler scheduler;
    private final int maxFailures;
    private final Duration callTimeout;
    private final Duration resetTimeout;
    private final Duration maxResetTimeout;
    private final double exponentialBackoffFactor;
    private final double randomFactor;
    private final Executor executor;

    private final AtomicReference<Duration> currentResetTimeout;
    private final AtomicReference<State> currentState = new AtomicReference<>();

    private final CopyOnWriteArrayList<Runnable> onOpenListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> onHalfOpenListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Runnable> onCloseListeners = new CopyOnWriteArrayList<>();

    /** Closed 状态单例。 */
    private final ClosedState closedState = new ClosedState();
    /** Open 状态单例。 */
    private final OpenState openState = new OpenState();
    /** HalfOpen 状态单例。 */
    private final HalfOpenState halfOpenState = new HalfOpenState();

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
        if (randomFactor < 0.0 || randomFactor > 1.0) {
            throw new IllegalArgumentException("randomFactor must be between 0.0 and 1.0");
        }
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.maxFailures = maxFailures;
        this.callTimeout = callTimeout;
        this.resetTimeout = resetTimeout;
        this.maxResetTimeout = maxResetTimeout;
        this.exponentialBackoffFactor = exponentialBackoffFactor;
        this.randomFactor = randomFactor;
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.currentResetTimeout = new AtomicReference<>(resetTimeout);
        closedState._enter();
        currentState.set(closedState);
    }

    /**
     * 创建默认在调用线程/同步执行器上运行回调的 {@link CircuitBreaker}。
     */
    public static CircuitBreaker create(Scheduler scheduler, int maxFailures, Duration callTimeout,
                                        Duration resetTimeout) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Duration.ofDays(36500), 1.0, 0.0, Runnable::run);
    }

    /** 配置指数退避因子（需 {@code >= 1.0}）。 */
    public CircuitBreaker withExponentialBackoff(Duration maxResetTimeout) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                Objects.requireNonNull(maxResetTimeout), 2.0, randomFactor, executor);
    }

    /** 配置随机抖动因子。 */
    public CircuitBreaker withRandomFactor(double randomFactor) {
        return new CircuitBreaker(scheduler, maxFailures, callTimeout, resetTimeout,
                maxResetTimeout, exponentialBackoffFactor, randomFactor, executor);
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
        return currentState.get() instanceof ClosedState;
    }

    /** 当前是否处于 {@code Open} 状态。 */
    public boolean isOpen() {
        return currentState.get() instanceof OpenState;
    }

    /** 当前是否处于 {@code HalfOpen} 状态。 */
    public boolean isHalfOpen() {
        return currentState.get() instanceof HalfOpenState;
    }

    /** 返回当前连续失败计数。 */
    public long currentFailureCount() {
        return currentState.get() instanceof ClosedState closed ? closed.get() : 0L;
    }

    /** 手动记录一次成功。 */
    public void succeed() {
        currentState.get().callSucceeds();
    }

    /** 手动记录一次失败。 */
    public void fail() {
        currentState.get().callFails();
    }

    /**
     * 通过熔断器保护异步调用。
     */
    public <T> CompletionStage<T> callWithCircuitBreaker(Supplier<CompletionStage<T>> body) {
        return callWithCircuitBreaker(body, (value, error) -> error != null);
    }

    /**
     * 通过熔断器保护异步调用，并使用自定义失败判定器。
     */
    public <T> CompletionStage<T> callWithCircuitBreaker(Supplier<CompletionStage<T>> body,
                                                         BiFunction<T, Throwable, Boolean> defineFailure) {
        State state = currentState.get();
        if (state instanceof ClosedState) {
            return callThrough(body, defineFailure);
        } else if (state instanceof OpenState) {
            return CompletableFuture.failedFuture(new CircuitBreakerOpenException(openState.remainingDuration()));
        } else if (state instanceof HalfOpenState half) {
            if (half.tryAcquire()) {
                return callThrough(body, defineFailure);
            }
            return CompletableFuture.failedFuture(new CircuitBreakerOpenException(Duration.ZERO));
        }
        return CompletableFuture.failedFuture(new IllegalStateException("Unknown state: " + state));
    }

    /**
     * 通过熔断器保护同步调用。
     */
    public <T> T callWithSyncCircuitBreaker(Callable<T> body) throws Exception {
        State state = currentState.get();
        if (state instanceof ClosedState || (state instanceof HalfOpenState half && half.tryAcquire())) {
            return executeSync(body);
        } else if (state instanceof OpenState) {
            throw new CircuitBreakerOpenException(openState.remainingDuration());
        } else if (state instanceof HalfOpenState) {
            throw new CircuitBreakerOpenException(Duration.ZERO);
        }
        throw new IllegalStateException("Unknown state: " + state);
    }

    /**
     * 核心异步调用路径，对齐上游 {@code State.callThrough}。
     */
    private <T> CompletionStage<T> callThrough(Supplier<CompletionStage<T>> body,
                                               BiFunction<T, Throwable, Boolean> defineFailure) {
        CompletionStage<T> stage;
        try {
            stage = body.get();
        } catch (Throwable error) {
            if (!NonFatal.apply(error)) throw error;
            currentState.get().callFails();
            return CompletableFuture.failedFuture(error);
        }
        if (callTimeout.isZero()) {
            return stage.whenComplete((value, error) -> {
                if (error != null) {
                    currentState.get().callFails();
                } else {
                    currentState.get().callSucceeds();
                }
            });
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        var timeoutTask = scheduler.scheduleOnce(callTimeout, () ->
                result.completeExceptionally(new TimeoutException("Call exceeded callTimeout of " + callTimeout)), executor);
        stage.whenComplete((value, error) -> {
            if (error != null) {
                result.completeExceptionally(error);
            } else {
                result.complete(value);
            }
            timeoutTask.cancel();
        });
        return result.whenComplete((value, error) -> {
            if (Boolean.TRUE.equals(defineFailure.apply(value, error))) {
                currentState.get().callFails();
            } else {
                currentState.get().callSucceeds();
            }
        });
    }

    private <T> T executeSync(Callable<T> body) throws Exception {
        try {
            T result = body.call();
            currentState.get().callSucceeds();
            return result;
        } catch (Throwable error) {
            currentState.get().callFails();
            if (error instanceof Exception ex) throw ex;
            if (error instanceof Error fatal) throw fatal;
            throw new RuntimeException(error);
        }
    }

    private void tripBreaker(State fromState) {
        if (currentState.compareAndSet(fromState, openState)) {
            openState.enter();
        }
    }

    private void resetBreaker() {
        if (currentState.compareAndSet(halfOpenState, closedState)) {
            closedState.enter();
        }
    }

    private void attemptReset() {
        if (currentState.compareAndSet(openState, halfOpenState)) {
            halfOpenState.enter();
        }
    }

    private void swapResetTimeout(Duration expected, Duration newTimeout) {
        currentResetTimeout.compareAndSet(expected, newTimeout);
    }

    private void notifyListeners(CopyOnWriteArrayList<Runnable> listeners) {
        for (Runnable listener : listeners) {
            executor.execute(listener);
        }
    }

    // ── State Implementations ──────────────────────────────────────────────────

    final class ClosedState extends AtomicInteger implements State {
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        public void addListener(Runnable listener) {
            listeners.add(listener);
        }

        @Override
        public void enter() {
            _enter();
            notifyListeners(listeners);
            notifyListeners(onCloseListeners);
        }

        void _enter() {
            set(0);
            swapResetTimeout(currentResetTimeout.get(), resetTimeout);
        }

        @Override
        public void callSucceeds() {
            set(0);
        }

        @Override
        public void callFails() {
            if (incrementAndGet() == maxFailures) {
                tripBreaker(this);
            }
        }
    }

    final class OpenState extends AtomicLong implements State {
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        public void addListener(Runnable listener) {
            listeners.add(listener);
        }

        @Override
        public void enter() {
            _enter();
            notifyListeners(listeners);
            notifyListeners(onOpenListeners);
        }

        void _enter() {
            set(System.nanoTime());
            Duration currentTimeout = currentResetTimeout.get();
            scheduler.scheduleOnce(currentTimeout, () -> attemptReset(), executor);
            double rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor;
            double nextNanos = currentTimeout.toNanos() * exponentialBackoffFactor * rnd;
            if (Double.isFinite(nextNanos) && nextNanos < maxResetTimeout.toNanos()) {
                Duration nextTimeout = Duration.ofNanos((long) nextNanos);
                swapResetTimeout(currentTimeout, nextTimeout);
            }
        }

        Duration remainingDuration() {
            long fromOpened = System.nanoTime() - get();
            long diff = currentResetTimeout.get().toNanos() - fromOpened;
            return diff <= 0L ? Duration.ZERO : Duration.ofNanos(diff);
        }

        @Override
        public void callSucceeds() { /* no-op */ }

        @Override
        public void callFails() { /* no-op */ }
    }

    final class HalfOpenState extends AtomicBoolean implements State {
        private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

        public void addListener(Runnable listener) {
            listeners.add(listener);
        }

        @Override
        public void enter() {
            _enter();
            notifyListeners(listeners);
            notifyListeners(onHalfOpenListeners);
        }

        void _enter() {
            set(true);
        }

        boolean tryAcquire() {
            return compareAndSet(true, false);
        }

        @Override
        public void callSucceeds() {
            resetBreaker();
        }

        @Override
        public void callFails() {
            tripBreaker(this);
        }
    }
}
