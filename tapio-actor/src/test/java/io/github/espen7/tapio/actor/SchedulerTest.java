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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.dispatch.ExecutionContext;
import io.github.espen7.tapio.event.LoggingAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerTest {
    private static final Duration TICK = Duration.ofMillis(10);
    private static final Duration HOUR = Duration.ofHours(1);
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;
    private ManualSchedulerHarness harness;

    @BeforeEach
    void createScheduler() {
        system = ActorSystem.create("periodic-scheduler");
        harness = new ManualSchedulerHarness(system.log());
    }

    @AfterEach
    void stopScheduler() throws Exception {
        if (harness != null) harness.close();
        if (system != null) system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void fixedDelayWaitsForFullIntervalAfterCompletion() {
        ControlledExecutor executor = new ControlledExecutor();
        AtomicInteger runs = new AtomicInteger();
        Cancellable task = harness.scheduler().scheduleWithFixedDelay(
                TICK.multipliedBy(2), TICK.multipliedBy(3), runs::incrementAndGet, executor);

        harness.primeQueue();
        harness.advance(TICK.multipliedBy(2));
        executor.take().run();
        assertEquals(1, runs.get());

        harness.primeQueue();
        harness.advance(TICK.multipliedBy(2));
        assertTrue(executor.tasks.isEmpty());
        harness.advance(TICK);
        executor.take().run();
        assertEquals(2, runs.get());
        assertTrue(task.cancel());
    }

    @Test
    void fixedRateCatchesUpAfterLongExecutionWithoutOverlap() {
        ControlledExecutor executor = new ControlledExecutor();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        AtomicInteger runs = new AtomicInteger();
        Cancellable task = harness.scheduler().scheduleAtFixedRate(
                TICK.multipliedBy(2), TICK.multipliedBy(3), () -> {
                    maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                    runs.incrementAndGet();
                    active.decrementAndGet();
                }, executor);

        harness.primeQueue();
        harness.advance(TICK.multipliedBy(2));
        Runnable first = executor.take();
        harness.advance(TICK.multipliedBy(7));
        first.run();
        assertEquals(1, runs.get());

        harness.primeQueue();
        executor.take().run();
        harness.primeQueue();
        executor.take().run();
        assertEquals(3, runs.get());
        assertEquals(1, maximumActive.get());
        assertTrue(task.cancel());
    }

    @Test
    void fixedRateUsesRemainingIdealInterval() {
        ControlledExecutor executor = new ControlledExecutor();
        AtomicInteger runs = new AtomicInteger();
        Cancellable task = harness.scheduler().scheduleAtFixedRate(
                TICK.multipliedBy(2), TICK.multipliedBy(3), runs::incrementAndGet, executor);

        harness.primeQueue();
        harness.advance(TICK.multipliedBy(2));
        Runnable first = executor.take();
        harness.advance(TICK);
        first.run();

        harness.primeQueue();
        harness.advance(TICK);
        assertTrue(executor.tasks.isEmpty());
        harness.advance(TICK);
        executor.take().run();
        assertEquals(2, runs.get());
        assertTrue(task.cancel());
    }

    @Test
    void periodicApisRejectNonPositiveIntervalsAndOverlongDelays() {
        ControlledExecutor executor = new ControlledExecutor();
        CaptureActorRef receiver = new CaptureActorRef();
        for (Duration interval : List.of(Duration.ZERO, Duration.ofNanos(-1))) {
            assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleWithFixedDelay(
                    TICK, interval, () -> {}, executor));
            assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleWithFixedDelay(
                    TICK, interval, receiver, "message", executor, ActorRef.noSender));
            assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleAtFixedRate(
                    TICK, interval, () -> {}, executor));
            assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleAtFixedRate(
                    TICK, interval, receiver, "message", executor, ActorRef.noSender));
        }

        Duration maximum = Duration.ofNanos(TICK.toNanos() * Integer.MAX_VALUE);
        Duration tooLong = maximum.plus(TICK);
        assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleWithFixedDelay(
                tooLong, TICK, () -> {}, executor));
        assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleWithFixedDelay(
                TICK, tooLong, () -> {}, executor));
        assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleAtFixedRate(
                tooLong, TICK, () -> {}, executor));
        assertThrows(IllegalArgumentException.class, () -> harness.scheduler().scheduleAtFixedRate(
                TICK, tooLong, () -> {}, executor));
        assertTrue(executor.tasks.isEmpty());
        assertEquals(100.0, harness.scheduler().maxFrequency());
    }

    @Test
    void maximumDelayIsInclusiveAndSubTickIntervalsAreAccepted() {
        ControlledExecutor executor = new ControlledExecutor();
        Duration maximum = Duration.ofNanos(TICK.toNanos() * Integer.MAX_VALUE);
        Cancellable delay = harness.scheduler().scheduleWithFixedDelay(maximum, TICK, () -> {}, executor);
        Cancellable rate = harness.scheduler().scheduleAtFixedRate(maximum, TICK, () -> {}, executor);
        assertTrue(delay.cancel());
        assertTrue(rate.cancel());

        ControlledExecutor subTickExecutor = new ControlledExecutor();
        AtomicInteger runs = new AtomicInteger();
        Cancellable subTickDelay = harness.scheduler().scheduleWithFixedDelay(
                Duration.ZERO, Duration.ofNanos(1), runs::incrementAndGet, subTickExecutor);
        subTickExecutor.take().run();
        assertTrue(subTickDelay.cancel());
        ControlledExecutor rateExecutor = new ControlledExecutor();
        Cancellable subTickRate = harness.scheduler().scheduleAtFixedRate(
                Duration.ZERO, Duration.ofNanos(1), runs::incrementAndGet, rateExecutor);
        rateExecutor.take().run();
        assertTrue(subTickRate.cancel());
        assertEquals(2, runs.get());
    }

    @Test
    void cancellingPeriodicTaskRunsSchedulerTaskCallbackOnExecutor() {
        for (boolean fixedRate : List.of(false, true)) {
            ControlledExecutor executor = new ControlledExecutor();
            RecordingTask recordingTask = new RecordingTask();
            Cancellable task = fixedRate
                    ? harness.scheduler().scheduleAtFixedRate(TICK, HOUR, recordingTask, executor)
                    : harness.scheduler().scheduleWithFixedDelay(TICK, HOUR, recordingTask, executor);

            assertTrue(task.cancel());
            assertTrue(task.isCancelled());
            assertFalse(task.cancel());
            executor.take().run();
            assertEquals(1, recordingTask.cancellations.get());
            assertEquals(0, recordingTask.runs.get());
            assertTrue(executor.tasks.isEmpty());
        }
    }

    @Test
    void cancellingRunningTaskPreventsNextExecutionWithoutCancellationCallback() throws Exception {
        for (boolean fixedRate : List.of(false, true)) {
            ControlledExecutor executor = new ControlledExecutor();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            RecordingTask recordingTask = new RecordingTask(() -> {
                started.countDown();
                try {
                    assertTrue(release.await(WAIT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(error);
                }
            });
            Cancellable task = fixedRate
                    ? harness.scheduler().scheduleAtFixedRate(Duration.ZERO, TICK, recordingTask, executor)
                    : harness.scheduler().scheduleWithFixedDelay(Duration.ZERO, TICK, recordingTask, executor);
            Thread runner = new Thread(executor.take(), "periodic-task-runner");
            runner.start();
            assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS));

            assertTrue(task.cancel());
            assertTrue(task.isCancelled());
            release.countDown();
            runner.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            assertFalse(runner.isAlive());
            assertEquals(1, recordingTask.runs.get());
            assertEquals(0, recordingTask.cancellations.get());
            assertTrue(executor.tasks.isEmpty());
        }
    }

    @Test
    void taskExceptionStopsRepetitionWithoutMarkingHandleCancelled() {
        for (boolean fixedRate : List.of(false, true)) {
            ControlledExecutor executor = new ControlledExecutor();
            IllegalStateException failure = new IllegalStateException("task failed");
            Cancellable task = fixedRate
                    ? harness.scheduler().scheduleAtFixedRate(Duration.ZERO, TICK, () -> {
                        throw failure;
                    }, executor)
                    : harness.scheduler().scheduleWithFixedDelay(Duration.ZERO, TICK, () -> {
                        throw failure;
                    }, executor);

            assertSame(failure, assertThrows(IllegalStateException.class, executor.take()::run));
            assertTrue(executor.tasks.isEmpty());
            assertFalse(task.isCancelled());
            assertTrue(task.cancel());
        }
    }

    @Test
    void fixedDelaySwallowsOnlySchedulerStateFailures() {
        ControlledExecutor executor = new ControlledExecutor();
        Cancellable schedulerFailure = harness.scheduler().scheduleWithFixedDelay(
                Duration.ZERO, TICK, () -> {
                    throw new IllegalStateException(new SchedulerException("closed"));
                }, executor);
        assertDoesNotThrow(executor.take()::run);
        assertTrue(executor.tasks.isEmpty());
        assertTrue(schedulerFailure.cancel());

        ControlledExecutor otherExecutor = new ControlledExecutor();
        Cancellable otherFailure = harness.scheduler().scheduleWithFixedDelay(
                Duration.ZERO, TICK, () -> {
                    throw new IllegalStateException("application failure");
                }, otherExecutor);
        assertThrows(IllegalStateException.class, otherExecutor.take()::run);
        assertTrue(otherFailure.cancel());
    }

    @Test
    void actorMessageOverloadsPreserveSenderAndStopAfterTermination() {
        ControlledExecutor executor = new ControlledExecutor();
        CaptureActorRef receiver = new CaptureActorRef();
        ActorRef sender = new CaptureActorRef();
        Cancellable task = harness.scheduler().scheduleAtFixedRate(
                Duration.ZERO, HOUR, receiver, "message", executor, sender);
        executor.take().run();
        assertEquals(List.of("message"), receiver.messages);
        assertEquals(List.of(sender), receiver.senders);
        assertTrue(task.cancel());

        CaptureActorRef terminated = new CaptureActorRef();
        terminated.terminateAfterTell = true;
        Cancellable delay = harness.scheduler().scheduleWithFixedDelay(
                Duration.ZERO, HOUR, terminated, "last", Runnable::run, sender);
        Cancellable rate = harness.scheduler().scheduleAtFixedRate(
                Duration.ZERO, HOUR, terminated, "last", Runnable::run, sender);
        assertEquals(List.of("last", "last"), terminated.messages);
        assertEquals(List.of(sender, sender), terminated.senders);
        assertFalse(delay.isCancelled());
        assertFalse(rate.isCancelled());
        assertTrue(delay.cancel());
        assertTrue(rate.cancel());
    }

    @Test
    void atomicCancellablePublishesFirstTaskWithoutOverwritingInlineReplacement() {
        AtomicBoolean firstCancelled = new AtomicBoolean();
        AtomicBoolean replacementCancelled = new AtomicBoolean();
        Cancellable first = testCancellable(firstCancelled);
        Cancellable replacement = testCancellable(replacementCancelled);

        Scheduler.AtomicCancellable task = new Scheduler.AtomicCancellable(Cancellable.initialNotCancelled) {
            @Override
            protected Cancellable scheduledFirst() {
                swap(replacement);
                return first;
            }
        };

        assertSame(replacement, task.get());
        assertFalse(firstCancelled.get());
        assertFalse(replacementCancelled.get());
        assertTrue(task.cancel());
        assertTrue(replacementCancelled.get());
        assertTrue(task.isCancelled());
        assertFalse(task.cancel());

        AtomicBoolean lateCancelled = new AtomicBoolean();
        task.swap(testCancellable(lateCancelled));
        assertTrue(lateCancelled.get());
    }

    @Test
    void schedulingAfterCloseIsRejected() {
        harness.close();

        assertThrows(IllegalStateException.class, () -> harness.scheduler().scheduleOnce(
                TICK, () -> {}, Runnable::run));
        assertThrows(IllegalStateException.class, () -> harness.scheduler().scheduleWithFixedDelay(
                TICK, TICK, () -> {}, Runnable::run));
        assertThrows(IllegalStateException.class, () -> harness.scheduler().scheduleAtFixedRate(
                TICK, TICK, () -> {}, Runnable::run));
    }

    @Test
    void atomicCancellableCanCancelRunningUnderlyingTask() {
        Scheduler.AtomicCancellable task = new Scheduler.AtomicCancellable(Cancellable.initialNotCancelled) {
            @Override
            protected Cancellable scheduledFirst() {
                return new Cancellable() {
                    @Override
                    public boolean cancel() {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }
                };
            }
        };

        assertTrue(task.cancel());
        assertTrue(task.isCancelled());
    }

    private static Cancellable testCancellable(AtomicBoolean cancelled) {
        return new Cancellable() {
            @Override
            public boolean cancel() {
                return !cancelled.getAndSet(true);
            }

            @Override
            public boolean isCancelled() {
                return cancelled.get();
            }
        };
    }

    private static final class RecordingTask implements SchedulerTask {
        private final AtomicInteger runs = new AtomicInteger();
        private final AtomicInteger cancellations = new AtomicInteger();
        private final Runnable body;

        private RecordingTask() {
            this(() -> {});
        }

        private RecordingTask(Runnable body) {
            this.body = body;
        }

        @Override
        public void run() {
            runs.incrementAndGet();
            body.run();
        }

        @Override
        public void cancelled() {
            cancellations.incrementAndGet();
        }
    }

    private static final class CaptureActorRef extends MinimalActorRef {
        private final ActorPath path = new ActorPath("tapio://periodic-scheduler/capture");
        private final List<Object> messages = new ArrayList<>();
        private final List<ActorRef> senders = new ArrayList<>();
        private volatile boolean terminateAfterTell;

        @Override
        public ActorRefProvider provider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ActorPath path() {
            return path;
        }

        @Override
        public void tell(Object message, ActorRef sender) {
            messages.add(message);
            senders.add(sender);
        }

        @Override
        public boolean isTerminated() {
            return terminateAfterTell;
        }
    }

    private static final class ControlledExecutor implements Executor, ExecutionContext {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        private final BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        @Override
        public void reportFailure(Throwable cause) {
            failures.add(cause);
        }

        private Runnable take() {
            try {
                Runnable task = tasks.poll(WAIT_SECONDS, TimeUnit.SECONDS);
                assertNotNull(task);
                return task;
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }
    }

    private static final class ManualSchedulerHarness {
        private final BlockingQueue<Long> advances = new LinkedBlockingQueue<>();
        private final BlockingQueue<Long> waits = new LinkedBlockingQueue<>();
        private final Scheduler scheduler;
        private volatile long time;
        private volatile boolean closed;

        private ManualSchedulerHarness(LoggingAdapter log) {
            Config config = ConfigFactory.parseString("tapio.scheduler.tick-duration = 10ms")
                    .withFallback(ConfigFactory.defaultReference());
            scheduler = new LightArrayRevolverScheduler(config, log, runnable -> {
                Thread thread = new Thread(runnable, "manual-scheduler-test");
                thread.setDaemon(true);
                return thread;
            }) {
                @Override
                protected long clock() {
                    return time;
                }

                @Override
                protected void waitNanos(long nanos) {
                    if (closed) return;
                    waits.add(nanos);
                    try {
                        time += advances.take();
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                    }
                }
            };
            awaitWait();
        }

        private Scheduler scheduler() {
            return scheduler;
        }

        private void advance(Duration duration) {
            advances.add(duration.toNanos());
            awaitWait();
        }

        private void primeQueue() {
            advance(Duration.ZERO);
        }

        private void awaitWait() {
            try {
                assertNotNull(waits.poll(WAIT_SECONDS, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }

        private void close() {
            if (closed) return;
            closed = true;
            advances.offer(0L);
            scheduler.close();
        }
    }
}
