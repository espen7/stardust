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
package io.github.espen7.tapio.dispatch;

import io.github.espen7.tapio.annotation.InternalApi;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 为每个任务创建虚拟线程的执行器，并在关闭时按需级联关闭底层池
 * {@code VirtualizedExecutorService}。
 *
 * <p>INTERNAL API。</p>
 */
@InternalApi
public final class VirtualizedExecutorService implements ExecutorService, LoadMetrics {
    private static final int UNDERLYING_RUNNING = 0;
    private static final int UNDERLYING_SHUTDOWN = 1;
    private static final int UNDERLYING_SHUTDOWN_NOW = 2;

    private final ExecutorService underlying;
    private final boolean cascadeShutdown;
    private final ExecutorService executor;
    private final AtomicBoolean underlyingShutdownScheduled = new AtomicBoolean(false);
    private final AtomicBoolean underlyingShutdownNowRequested = new AtomicBoolean(false);
    private final AtomicInteger underlyingShutdownState = new AtomicInteger(UNDERLYING_RUNNING);

    /**
     * 创建虚拟化执行器。
     *
     * @param vtFactory 虚拟线程工厂
     * @param underlying 底层池（可为 {@code null}）
     * @param cascadeShutdown 关闭后是否级联关闭底层池
     */
    public VirtualizedExecutorService(ThreadFactory vtFactory, ExecutorService underlying, boolean cascadeShutdown) {
        if (!VirtualThreadSupport.isSupported) {
            throw new IllegalArgumentException("Virtual thread is not supported.");
        }
        Objects.requireNonNull(vtFactory, "Virtual thread factory must not be null");
        this.underlying = underlying;
        this.cascadeShutdown = cascadeShutdown;
        this.executor = VirtualThreadSupport.newThreadPerTaskExecutor(vtFactory);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        shutdownUnderlyingWhenVirtualThreadsHaveTerminated();
    }

    @Override
    public List<Runnable> shutdownNow() {
        underlyingShutdownNowRequested.set(true);
        List<Runnable> result = executor.shutdownNow();
        shutdownUnderlyingWhenVirtualThreadsHaveTerminated();
        return result;
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        if (cascadeShutdown) {
            if (executor.isTerminated()) {
                shutdownUnderlying();
            }
            return executor.isTerminated() && (underlying == null || underlying.isTerminated());
        }
        return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (cascadeShutdown) {
            long timeoutNanos = unit.toNanos(timeout);
            long deadline = System.nanoTime() + timeoutNanos;
            if (!executor.awaitTermination(timeout, unit)) {
                return false;
            }
            shutdownUnderlying();
            if (underlying == null) {
                return true;
            }
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0L) {
                return underlying.isTerminated();
            }
            return underlying.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
        }
        return executor.awaitTermination(timeout, unit);
    }

    private void shutdownUnderlyingWhenVirtualThreadsHaveTerminated() {
        if (cascadeShutdown && underlying != null) {
            if (executor.isTerminated()) {
                shutdownUnderlying();
            } else if (underlyingShutdownScheduled.compareAndSet(false, true)) {
                VirtualThreadSupport.startVirtualThread("tapio-virtualized-executor-shutdown", () -> {
                    boolean interrupted = false;
                    try {
                        while (!executor.isTerminated()) {
                            try {
                                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                            } catch (InterruptedException ignored) {
                                interrupted = true;
                            }
                        }
                    } finally {
                        shutdownUnderlying();
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
        }
    }

    private void shutdownUnderlying() {
        if (cascadeShutdown && underlying != null && underlyingShutdownNowRequested.get()) {
            if (underlyingShutdownState.getAndSet(UNDERLYING_SHUTDOWN_NOW) != UNDERLYING_SHUTDOWN_NOW) {
                underlying.shutdownNow();
            }
        } else if (cascadeShutdown && underlying != null
                && underlyingShutdownState.compareAndSet(UNDERLYING_RUNNING, UNDERLYING_SHUTDOWN)) {
            underlying.shutdown();
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public boolean atFullThrottle() {
        return underlying instanceof LoadMetrics metrics && metrics.atFullThrottle();
    }
}
