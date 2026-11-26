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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DispatcherExecutorConfigTest {
    private static final String ID = "test-dispatcher";
    private static final long WAIT_SECONDS = 3;

    @Test
    void defaultExecutorFallsBackToForkJoinWithDefaultParallelism() throws Exception {
        ExecutorService executor = factory(config(Map.of())).createExecutorService();
        try {
            ForkJoinPool pool = assertInstanceOf(ForkJoinPool.class, executor);
            assertEquals(ThreadPoolConfig.scaledPoolSize(8, 1.0, 64), pool.getParallelism());
            assertTrue(pool.getAsyncMode());
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void defaultExecutorCanFallBackToThreadPool() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.default-executor.fallback", "thread-pool-executor",
                "tapio.actor.default-dispatcher.thread-pool-executor.fixed-pool-size", "2"));
        ExecutorService pool = assertInstanceOf(ThreadPoolExecutor.class, factory(config).createExecutorService());
        try {
            assertEquals(2, ((ThreadPoolExecutor) pool).getCorePoolSize());
            assertEquals(2, ((ThreadPoolExecutor) pool).getMaximumPoolSize());
        } finally {
            shutdown(pool);
        }
    }

    @Test
    void defaultExecutorUsesPassedExecutionContextWithoutOwningItsLifecycle() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.default-executor.fallback", "thread-pool-executor"));
        ExecutorService external = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> reported = new AtomicReference<>();
        ExecutionContext executionContext = new ExecutionContext() {
            @Override
            public void execute(Runnable command) {
                external.execute(command);
            }

            @Override
            public void reportFailure(Throwable cause) {
                reported.set(cause);
            }
        };
        try {
            DispatcherPrerequisites prerequisites = new DefaultDispatcherPrerequisites(
                    new MonitorableThreadFactory("executor-test", false, getClass().getClassLoader(), null),
                    new EventStream(LogEvent.Level.ERROR), null, null, null, Optional.of(executionContext));
            ExecutorService adapter = factory(config, prerequisites).createExecutorService();
            CountDownLatch done = new CountDownLatch(1);

            adapter.execute(done::countDown);
            assertTrue(done.await(WAIT_SECONDS, TimeUnit.SECONDS));
            adapter.shutdown();

            assertFalse(adapter.isShutdown());
            assertFalse(adapter.isTerminated());
            assertFalse(adapter.awaitTermination(1, TimeUnit.MILLISECONDS));
            assertTrue(adapter.shutdownNow().isEmpty());
            assertFalse(external.isShutdown());
            assertNull(reported.get());
        } finally {
            shutdown(external);
        }
    }

    @Test
    void forkJoinHonorsPeekingModePoolLimitsAndThreadFactory() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "fork-join-executor",
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-min", 2,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-factor", 0.0,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-max", 4,
                "tapio.actor.default-dispatcher.fork-join-executor.task-peeking-mode", "LIFO",
                "tapio.actor.default-dispatcher.fork-join-executor.maximum-pool-size", 8,
                "tapio.actor.default-dispatcher.fork-join-executor.minimum-runnable", 3));
        AtomicReference<Thread> worker = new AtomicReference<>();
        Thread.UncaughtExceptionHandler handler = (thread, cause) -> {};
        MonitorableThreadFactory threadFactory = new MonitorableThreadFactory("executor-test", true,
                getClass().getClassLoader(), handler);
        ExecutorService executor = factory(config, threadFactory).createExecutorService();
        try {
            ForkJoinPool pool = assertInstanceOf(ForkJoinPool.class, executor);
            assertEquals(2, pool.getParallelism());
            assertFalse(pool.getAsyncMode());
            CountDownLatch done = new CountDownLatch(1);
            pool.execute(() -> {
                worker.set(Thread.currentThread());
                done.countDown();
            });
            assertTrue(done.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertEquals("executor-test-" + ID + "-1", worker.get().getName());
            assertTrue(worker.get().isDaemon());
            assertSame(getClass().getClassLoader(), worker.get().getContextClassLoader());
            assertSame(handler, worker.get().getUncaughtExceptionHandler());
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void forkJoinPreservesExistingTaskCancellationAndCompletion() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "fork-join-executor",
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-min", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-factor", 0.0,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-max", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.maximum-pool-size", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.minimum-runnable", 0));
        ForkJoinPool pool = (ForkJoinPool) factory(config).createExecutorService();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean cancelledTaskRan = new AtomicBoolean();
        try {
            pool.execute(() -> {
                started.countDown();
                await(release);
            });
            assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));
            ForkJoinTask<Integer> cancelled = new RecursiveTask<>() {
                @Override
                protected Integer compute() {
                    cancelledTaskRan.set(true);
                    return 1;
                }
            };
            pool.execute(cancelled);
            assertTrue(cancelled.cancel(false));
            release.countDown();
            assertTrue(cancelled.isCancelled());
            assertFalse(cancelledTaskRan.get());

            ForkJoinTask<String> completed = new RecursiveTask<>() {
                @Override
                protected String compute() {
                    return "done";
                }
            };
            pool.execute(completed);
            assertEquals("done", completed.get(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            shutdown(pool);
        }
    }

    @Test
    void forkJoinWorkerCanReuseRunnableAfterFailure() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "fork-join-executor",
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-min", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-factor", 0.0,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-max", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.maximum-pool-size", 1,
                "tapio.actor.default-dispatcher.fork-join-executor.minimum-runnable", 0));
        RuntimeException failure = new IllegalStateException("expected failure");
        AtomicReference<Throwable> uncaught = new AtomicReference<>();
        CountDownLatch failed = new CountDownLatch(1);
        CountDownLatch succeeded = new CountDownLatch(1);
        MonitorableThreadFactory threadFactory = new MonitorableThreadFactory("executor-test", true,
                getClass().getClassLoader(), (thread, cause) -> {
                    uncaught.set(cause);
                    failed.countDown();
                });
        ExecutorService executor = factory(config, threadFactory).createExecutorService();
        AtomicInteger attempts = new AtomicInteger();
        Runnable runnable = () -> {
            if (attempts.incrementAndGet() == 1) throw failure;
            succeeded.countDown();
        };
        try {
            executor.execute(runnable);
            assertTrue(failed.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertSame(failure, uncaught.get());
            executor.execute(runnable);
            assertTrue(succeeded.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertEquals(2, attempts.get());
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void monitorableThreadFactorySharesCounterAcrossDerivedNames() {
        MonitorableThreadFactory factory = new MonitorableThreadFactory("base", true,
                DispatcherExecutorConfigTest.class.getClassLoader(), null);
        Thread first = factory.newThread(() -> {});
        Thread second = factory.withName("derived").newThread(() -> {});

        assertEquals("base-1", first.getName());
        assertEquals("derived-2", second.getName());
        assertTrue(first.isDaemon());
        assertTrue(second.isDaemon());
        assertSame(DispatcherExecutorConfigTest.class.getClassLoader(), second.getContextClassLoader());
    }

    @Test
    void jdk25MinimumRunnableScalesToHalfParallelismWithinBounds() {
        assertEquals(1, ForkJoinExecutorConfigurator.resolveMinimumRunnable(-1, 1, 25));
        assertEquals(1, ForkJoinExecutorConfigurator.resolveMinimumRunnable(-1, 2, 25));
        assertEquals(2, ForkJoinExecutorConfigurator.resolveMinimumRunnable(-1, 4, 25));
        assertEquals(8, ForkJoinExecutorConfigurator.resolveMinimumRunnable(-1, 20, 25));
        assertEquals(0, ForkJoinExecutorConfigurator.resolveMinimumRunnable(0, 20, 25));
        assertEquals(1, ForkJoinExecutorConfigurator.resolveMinimumRunnable(-1, 20, 21));
    }

    @Test
    void threadPoolHonorsFixedSizeArrayQueueAndTimeout() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "thread-pool-executor",
                "tapio.actor.default-dispatcher.thread-pool-executor.fixed-pool-size", "3",
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-size", 7,
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-type", "array",
                "tapio.actor.default-dispatcher.thread-pool-executor.allow-core-timeout", false,
                "tapio.actor.default-dispatcher.thread-pool-executor.keep-alive-time", "2s"));
        ExecutorService executor = factory(config).createExecutorService();
        try {
            ThreadPoolExecutor pool = assertInstanceOf(ThreadPoolExecutor.class, executor);
            assertEquals(3, pool.getCorePoolSize());
            assertEquals(3, pool.getMaximumPoolSize());
            assertFalse(pool.allowsCoreThreadTimeOut());
            assertEquals(TimeUnit.SECONDS.toNanos(2), pool.getKeepAliveTime(TimeUnit.NANOSECONDS));
            ArrayBlockingQueue<?> queue = assertInstanceOf(ArrayBlockingQueue.class, pool.getQueue());
            assertEquals(7, queue.remainingCapacity());
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void saturatedThreadPoolRunsOnCallerUnlessShutdown() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "thread-pool-executor",
                "tapio.actor.default-dispatcher.thread-pool-executor.fixed-pool-size", "1",
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-size", 1,
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-type", "array"));
        ThreadPoolExecutor pool = (ThreadPoolExecutor) factory(config).createExecutorService();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean ranOnCaller = new AtomicBoolean();
        try {
            pool.execute(() -> {
                started.countDown();
                await(release);
            });
            assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));
            pool.execute(() -> {});
            pool.execute(() -> ranOnCaller.set(true));
            assertTrue(ranOnCaller.get());

            pool.shutdown();
            RejectedExecutionException error = assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {}));
            assertEquals("Shutdown", error.getMessage());
        } finally {
            release.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        }
    }

    @Test
    void unboundedQueueDoesNotValidateTaskQueueType() throws Exception {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "thread-pool-executor",
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-size", -1,
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-type", "ignored"));
        ExecutorService executor = factory(config).createExecutorService();
        try {
            assertInstanceOf(LinkedBlockingQueue.class, ((ThreadPoolExecutor) executor).getQueue());
        } finally {
            shutdown(executor);
        }
    }

    @Test
    void invalidExecutorSettingsFailWithSourceSemantics() {
        Config invalidForkJoin = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "fork-join-executor",
                "tapio.actor.default-dispatcher.fork-join-executor.task-peeking-mode", "LILO"));
        IllegalArgumentException forkJoinError = assertThrows(IllegalArgumentException.class,
                () -> factory(invalidForkJoin));
        assertEquals("Cannot instantiate ForkJoinExecutorServiceFactory. "
                + "\"task-peeking-mode\" in \"fork-join-executor\" section could only set to \"FIFO\" or \"LIFO\".",
                forkJoinError.getMessage());

        Config invalidQueue = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "thread-pool-executor",
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-size", 1,
                "tapio.actor.default-dispatcher.thread-pool-executor.task-queue-type", "stack"));
        IllegalArgumentException queueError = assertThrows(IllegalArgumentException.class, () -> factory(invalidQueue));
        assertEquals("[stack] is not a valid task-queue-type [array|linked]!", queueError.getMessage());
    }

    @Test
    void forkJoinRejectsMaximumPoolSizeBelowParallelism() {
        Config config = config(Map.of(
                "tapio.actor.default-dispatcher.executor", "fork-join-executor",
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-min", 2,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-factor", 0.0,
                "tapio.actor.default-dispatcher.fork-join-executor.parallelism-max", 2,
                "tapio.actor.default-dispatcher.fork-join-executor.maximum-pool-size", 1));
        ExecutorServiceFactory factory = factory(config);
        assertThrows(IllegalArgumentException.class, factory::createExecutorService);
    }

    @Test
    void unsupportedExecutorAndVirtualizationFailExplicitly() {
        ConfigurationException executorError = assertThrows(ConfigurationException.class,
                () -> factory(config(Map.of("tapio.actor.default-dispatcher.executor", "affinity-pool-executor"))));
        assertTrue(executorError.getMessage().contains("Supported built-ins"));

        ConfigurationException virtualError = assertThrows(ConfigurationException.class,
                () -> factory(config(Map.of("tapio.actor.default-dispatcher.fork-join-executor.virtualize", true))));
        assertEquals("Virtualized fork-join-executor is not supported", virtualError.getMessage());
    }

    private static Config config(Map<String, ?> overrides) {
        Config root = ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.defaultReference());
        return ConfigFactory.parseMap(Map.of("id", ID))
                .withFallback(root.getConfig(Dispatchers.DefaultDispatcherId))
                .withFallback(root);
    }

    private static ExecutorServiceFactory factory(Config config) {
        return factory(config, new MonitorableThreadFactory("executor-test", false,
                DispatcherExecutorConfigTest.class.getClassLoader(), null));
    }

    private static ExecutorServiceFactory factory(Config config, ThreadFactory threadFactory) {
        DispatcherPrerequisites prerequisites = new DefaultDispatcherPrerequisites(
                threadFactory, null, null, null, null, Optional.empty());
        return factory(config, prerequisites);
    }

    private static ExecutorServiceFactory factory(Config config, DispatcherPrerequisites prerequisites) {
        ThreadFactory threadFactory = prerequisites.threadFactory();
        MessageDispatcherConfigurator configurator = new MessageDispatcherConfigurator(config, prerequisites) {
            @Override
            public MessageDispatcher dispatcher() {
                throw new UnsupportedOperationException();
            }
        };
        return configurator.configureExecutor().createExecutorServiceFactory(config.getString("id"), threadFactory);
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) throw new AssertionError("latch timed out");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new AssertionError(error);
        }
    }
}
