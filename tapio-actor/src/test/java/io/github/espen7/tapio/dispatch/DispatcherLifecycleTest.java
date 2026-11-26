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
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefProvider;
import io.github.espen7.tapio.actor.ActorRefWithCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.Cancellable;
import io.github.espen7.tapio.actor.LightArrayRevolverScheduler;
import io.github.espen7.tapio.actor.LocalActorRef;
import io.github.espen7.tapio.actor.MinimalActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Scheduler;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DispatcherLifecycleTest {
    private static final Duration WAIT = Duration.ofSeconds(3);
    private static final Duration SHUTDOWN_DELAY = Duration.ofHours(1);

    private ActorSystem system;

    @AfterEach
    void closeSystem() {
        if (system != null && !system.getWhenTerminated().toCompletableFuture().isDone()) system.close();
    }

    @Test
    void executorIsLazyAndRecreatedAfterShutdown() throws Exception {
        system = ActorSystem.create("dispatcher-lazy");
        ManualScheduler scheduler = new ManualScheduler();
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = dispatcher(scheduler, factory);

        assertEquals(0, factory.creations.get());
        runAndAwaitCleanup(dispatcher, scheduler);
        assertEquals(1, factory.creations.get());
        ExecutorService firstExecutor = factory.executors.getFirst();

        dispatcher.shutdown();
        assertTrue(firstExecutor.isShutdown());

        runAndAwaitCleanup(dispatcher, scheduler);
        assertEquals(2, factory.creations.get());
        ExecutorService secondExecutor = factory.executors.get(1);
        assertNotSame(firstExecutor, secondExecutor);
        assertFalse(secondExecutor.isShutdown());
        dispatcher.close();
        assertTrue(secondExecutor.isShutdown());
    }

    @Test
    void ordinaryTasksBalanceInhabitantsAndReportNonFatalFailures() throws Exception {
        system = ActorSystem.create("dispatcher-tasks");
        ManualScheduler scheduler = new ManualScheduler();
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = dispatcher(scheduler, factory);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new CaptureActorRef(events), Error.class);
        AtomicInteger observedInhabitants = new AtomicInteger(-1);

        dispatcher.execute(() -> observedInhabitants.set((int) dispatcher.inhabitants()));
        await("ordinary task did not finish", () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);
        assertEquals(1, observedInhabitants.get());

        RuntimeException failure = new IllegalStateException("task failed");
        dispatcher.execute(() -> { throw failure; });
        Error event = (Error) events.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(event);
        await("failed task did not release inhabitants", () -> dispatcher.inhabitants() == 0);
        assertEquals("TaskInvocation", event.source());
        assertSame(TaskInvocation.class, event.sourceClass());
        assertEquals("task failed", event.message());
        assertSame(failure, event.cause());
        dispatcher.close();
    }

    @Test
    void shutdownFailureIsReportedThroughDispatcherExecutionContext() throws Exception {
        system = ActorSystem.create("dispatcher-shutdown-failure");
        ManualScheduler scheduler = new ManualScheduler();
        RuntimeException failure = new IllegalStateException("shutdown failed");
        Dispatcher dispatcher = dispatcher(scheduler, () -> new FailingShutdownExecutor(failure));
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new CaptureActorRef(events), Error.class);

        runAndAwaitCleanup(dispatcher, scheduler);
        scheduler.runNext();

        Error event = (Error) events.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(Dispatcher.class.getName(), event.source());
        assertSame(Dispatcher.class, event.sourceClass());
        assertEquals("shutdown failed", event.message());
        assertSame(failure, event.cause());
    }

    @Test
    void submissionFailureRollsBackInhabitantsAndReportsFirstRejection() {
        system = ActorSystem.create("dispatcher-rejected-task");
        ManualScheduler scheduler = new ManualScheduler();
        RejectingExecutor executor = new RejectingExecutor();
        Dispatcher dispatcher = dispatcher(scheduler, () -> executor);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new CaptureActorRef(events), Error.class);

        RejectedExecutionException thrown = assertThrows(RejectedExecutionException.class,
                () -> dispatcher.execute(() -> {}));

        assertEquals(2, executor.attempts.get());
        assertEquals("rejected-2", thrown.getMessage());
        assertEquals(0, dispatcher.inhabitants());
        Error event = (Error) events.poll();
        assertNotNull(event);
        assertEquals(Dispatcher.class.getName(), event.source());
        assertSame(Dispatcher.class, event.sourceClass());
        assertEquals("executeTask was rejected twice!", event.message());
        assertEquals("rejected-1", event.cause().getMessage());
        dispatcher.close();
    }

    @Test
    void mailboxSubmissionFailureReportsFirstRejectionAndRestoresIdleState() {
        system = ActorSystem.create("dispatcher-rejected-mailbox");
        ManualScheduler scheduler = new ManualScheduler();
        RejectingExecutor executor = new RejectingExecutor();
        Dispatcher dispatcher = dispatcher(scheduler, () -> executor);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(new CaptureActorRef(events), Error.class);
        Mailbox mailbox = new DefaultSystemMessageQueue(new UnboundedMailbox.MessageQueue()) {};

        RejectedExecutionException thrown = assertThrows(RejectedExecutionException.class,
                () -> dispatcher.registerForExecution(mailbox, true, true));

        assertEquals(2, executor.attempts.get());
        assertEquals("rejected-2", thrown.getMessage());
        assertFalse(mailbox.isScheduled());
        Error event = (Error) events.poll();
        assertNotNull(event);
        assertEquals(Dispatcher.class.getName(), event.source());
        assertSame(Dispatcher.class, event.sourceClass());
        assertEquals("registerForExecution was rejected twice!", event.message());
        assertEquals("rejected-2", event.cause().getMessage());
        dispatcher.close();
    }

    @Test
    void attachAndDetachCountActorOnceAndCloseOldMailbox() throws Exception {
        system = ActorSystem.create("dispatcher-attach");
        ManualScheduler scheduler = new ManualScheduler();
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = dispatcher(scheduler, factory);
        TestActor actor = detachedActor(dispatcher, "first");

        assertTrue(actor.ready.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(1, dispatcher.inhabitants());
        assertEquals("pong", actor.request());
        assertEquals(1, dispatcher.inhabitants());
        Mailbox oldMailbox = actor.ref.underlying().mailbox();

        actor.ref.stop();
        assertTrue(actor.stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("detach did not schedule shutdown", () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);
        assertTrue(oldMailbox.isClosed());
        assertSame(dispatcher.mailboxes.deadLetterMailbox(), actor.ref.underlying().mailbox());
    }

    @Test
    void rescheduledShutdownWaitsForASecondAction() throws Exception {
        system = ActorSystem.create("dispatcher-rescheduled");
        ManualScheduler scheduler = new ManualScheduler();
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = dispatcher(scheduler, factory);
        TestActor first = detachedActor(dispatcher, "first");
        assertTrue(first.ready.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));

        first.ref.stop();
        assertTrue(first.stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("first detach did not schedule shutdown", () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);

        TestActor second = detachedActor(dispatcher, "second");
        assertTrue(second.ready.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        second.ref.stop();
        assertTrue(second.stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("second detach did not mark shutdown rescheduled",
                () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);
        ExecutorService executor = factory.executors.getFirst();

        scheduler.runNext();
        assertEquals(1, scheduler.pendingCount());
        assertFalse(executor.isShutdown());

        scheduler.runNext();
        assertEquals(0, scheduler.pendingCount());
        assertTrue(executor.isShutdown());
    }

    @Test
    void schedulerCloseRunsPendingDispatcherShutdown() throws Exception {
        system = ActorSystem.create("dispatcher-close-task");
        ManualScheduler scheduler = new ManualScheduler();
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = dispatcher(scheduler, factory);
        TestActor actor = detachedActor(dispatcher, "actor");
        assertTrue(actor.ready.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        actor.ref.stop();
        assertTrue(actor.stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("shutdown action was not scheduled", () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);
        ExecutorService executor = factory.executors.getFirst();

        scheduler.close();

        assertTrue(scheduler.closed);
        assertEquals(0, scheduler.pendingCount());
        assertTrue(executor.isShutdown());
    }

    @Test
    void taskInvocationHandlesNullMessagesAndRethrowsFatalErrors() {
        EventStream eventStream = new EventStream(null);
        eventStream.setLogLevel(LogEvent.Level.ERROR);
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        eventStream.subscribe(new CaptureActorRef(events), Error.class);
        RuntimeException failure = new RuntimeException();
        AtomicBoolean cleaned = new AtomicBoolean();

        new TaskInvocation(eventStream, () -> { throw failure; }, () -> cleaned.set(true)).run();

        assertTrue(cleaned.get());
        Error event = (Error) events.poll();
        assertNotNull(event);
        assertEquals("null", event.message());
        assertSame(failure, event.cause());

        LinkageError fatal = new LinkageError("fatal");
        AtomicBoolean fatalCleaned = new AtomicBoolean();
        TaskInvocation invocation = new TaskInvocation(eventStream, () -> { throw fatal; },
                () -> fatalCleaned.set(true));
        assertSame(fatal, assertThrows(LinkageError.class, invocation::run));
        assertTrue(fatalCleaned.get());
        assertTrue(events.isEmpty());
    }

    @Test
    void recursiveFailureReportingDoesNotReenterEventStream() {
        EventStream eventStream = new EventStream(null);
        eventStream.setLogLevel(LogEvent.Level.ERROR);
        AtomicInteger deliveries = new AtomicInteger();
        ActorRef subscriber = new CaptureActorRef(message -> {
            deliveries.incrementAndGet();
            DispatchFailureReporting.report(eventStream, (LogEvent) message);
        });
        eventStream.subscribe(subscriber, Error.class);
        Error error = new Error("dispatcher", Dispatcher.class, "recursive", new IllegalStateException("recursive"));

        DispatchFailureReporting.report(eventStream, error);

        assertEquals(1, deliveries.get());
    }

    @Test
    void zeroTimeoutDispatcherCanBeReusedAndWatcherObservesDetachedMailbox() throws Exception {
        Config config = ConfigFactory.parseMap(Map.of(
                "dispatchers.reused.shutdown-timeout", "0ms",
                "tapio.log-dead-letters", "off"));
        system = ActorSystem.create("dispatcher-reuse", config);
        MessageDispatcher dispatcher = system.dispatchers().lookup("dispatchers.reused");
        CountDownLatch firstReady = new CountDownLatch(1);
        CountDownLatch firstStopped = new CountDownLatch(1);
        ActorRef first = system.actorOf(Props.create(LifecycleActor.class, firstReady, firstStopped)
                .withDispatcher("dispatchers.reused"), "first");
        BlockingQueue<ActorRef> terminated = new LinkedBlockingQueue<>();
        ActorRef watcherRef = system.actorOf(Props.create(WatcherProbe.class, terminated), "watcher");
        assertEquals("watching", ask(watcherRef, new WatchRequest(first)));
        assertTrue(firstReady.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals("pong", ask(first, "ping"));
        Mailbox oldMailbox = ((ActorCell) ((ActorRefWithCell) first).underlying()).mailbox();

        system.stop(first);

        assertSame(first, terminated.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(firstStopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(oldMailbox.isClosed());
        assertSame(dispatcher.mailboxes.deadLetterMailbox(), ((ActorCell) ((ActorRefWithCell) first).underlying()).mailbox());
        await("dispatcher did not return to zero inhabitants", () -> dispatcher.inhabitants() == 0);

        CountDownLatch secondReady = new CountDownLatch(1);
        CountDownLatch secondStopped = new CountDownLatch(1);
        ActorRef second = system.actorOf(Props.create(LifecycleActor.class, secondReady, secondStopped)
                .withDispatcher("dispatchers.reused"), "second");
        assertTrue(secondReady.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals("pong", ask(second, "ping"));
        assertEquals(1, dispatcher.inhabitants());
    }

    @Test
    void systemTerminationRunsPendingDispatcherShutdownBeforeCompleting() throws Exception {
        Config config = ConfigFactory.parseMap(Map.of(
                "tapio.scheduler.implementation", RecordingScheduler.class.getName(),
                "dispatchers.pending.shutdown-timeout", "1h",
                "tapio.log-dead-letters", "off"));
        system = ActorSystem.create("dispatcher-termination", config);
        RecordingScheduler scheduler = RecordingScheduler.LAST.get();
        MessageDispatcher dispatcher = system.dispatchers().lookup("dispatchers.pending");
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        ActorRef actor = system.actorOf(Props.create(LifecycleActor.class, ready, stopped)
                .withDispatcher("dispatchers.pending"), "actor");
        BlockingQueue<ActorRef> terminated = new LinkedBlockingQueue<>();
        ActorRef watcherRef = system.actorOf(Props.create(WatcherProbe.class, terminated), "watcher");
        assertEquals("watching", ask(watcherRef, new WatchRequest(actor)));
        assertTrue(ready.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals("pong", ask(actor, "ping"));

        system.stop(actor);

        assertSame(actor, terminated.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("named dispatcher did not detach", () -> dispatcher.inhabitants() == 0);
        assertEquals(1, scheduler.scheduledCloseTasks.get());
        assertEquals(0, scheduler.closeTaskRuns.get());

        system.terminate().toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);

        assertTrue(scheduler.closed);
        assertTrue(scheduler.closeTaskRuns.get() >= 1);
        assertTrue(system.getWhenTerminated().toCompletableFuture().isDone());
    }

    @Test
    void dispatchersShareTheSystemDeadLetterMailbox() {
        system = ActorSystem.create("dispatcher-shared-dead-letters", ConfigFactory.parseString("""
                dispatchers.other {
                  type = "Dispatcher"
                  throughput = 1
                }
                """));
        MessageDispatcher defaultDispatcher = system.dispatcher();
        MessageDispatcher otherDispatcher = system.dispatchers().lookup("dispatchers.other");

        assertSame(defaultDispatcher.mailboxes.deadLetterMailbox(), otherDispatcher.mailboxes.deadLetterMailbox());
        assertSame(defaultDispatcher.deadLetters(), otherDispatcher.deadLetters());
    }

    @Test
    void taskAfterSystemTerminationRecreatesAndShutsDownExecutor() throws Exception {
        system = ActorSystem.create("dispatcher-after-termination");
        CountingExecutorFactory factory = new CountingExecutorFactory();
        Dispatcher dispatcher = DispatcherTestSupport.dispatcher(system, system.scheduler(), factory);
        CountDownLatch firstRun = new CountDownLatch(1);
        dispatcher.execute(firstRun::countDown);
        assertTrue(firstRun.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("first task did not release inhabitants", () -> dispatcher.inhabitants() == 0);
        ExecutorService firstExecutor = factory.executors.getFirst();

        system.terminate().toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);

        assertTrue(firstExecutor.isShutdown());
        CountDownLatch secondRun = new CountDownLatch(1);
        dispatcher.execute(secondRun::countDown);
        assertTrue(secondRun.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(2, factory.creations.get());
        ExecutorService secondExecutor = factory.executors.get(1);
        await("closed scheduler did not shut down recreated executor", secondExecutor::isShutdown);
        assertEquals(0, dispatcher.inhabitants());
        assertTrue(secondExecutor.awaitTermination(WAIT.toMillis(), TimeUnit.MILLISECONDS));
    }

    private Dispatcher dispatcher(Scheduler scheduler, ExecutorServiceFactory factory) {
        return DispatcherTestSupport.dispatcher(system, scheduler, factory);
    }

    private TestActor detachedActor(Dispatcher dispatcher, String name) {
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        CaptureActorRef sender = new CaptureActorRef();
        LocalActorRef ref = new LocalActorRef((ActorSystemImpl) system,
                Props.create(LifecycleActor.class, ready, stopped), dispatcher, new UnboundedMailbox(),
                new CaptureActorRef(), new ActorPath("tapio://dispatcher-lifecycle/user/" + name), name);
        return new TestActor(ref, sender, ready, stopped);
    }

    private static void runAndAwaitCleanup(Dispatcher dispatcher, ManualScheduler scheduler) throws InterruptedException {
        CountDownLatch ran = new CountDownLatch(1);
        dispatcher.execute(ran::countDown);
        assertTrue(ran.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        await("task cleanup did not release inhabitants",
                () -> dispatcher.inhabitants() == 0 && scheduler.pendingCount() == 1);
    }

    private static Object ask(ActorRef actor, Object message) throws Exception {
        return Patterns.ask(actor, message, Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static void await(String message, BooleanSupplier condition) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail(message);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private static final class CountingExecutorFactory implements ExecutorServiceFactory {
        private final AtomicInteger creations = new AtomicInteger();
        private final List<ExecutorService> executors = new CopyOnWriteArrayList<>();

        @Override
        public ExecutorService createExecutorService() {
            creations.incrementAndGet();
            ThreadFactory threadFactory = runnable -> {
                Thread thread = new Thread(runnable, "dispatcher-lifecycle-test");
                thread.setDaemon(true);
                return thread;
            };
            ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
            executors.add(executor);
            return executor;
        }
    }

    private static final class FailingShutdownExecutor extends AbstractExecutorService {
        private final RuntimeException failure;

        private FailingShutdownExecutor(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
            throw failure;
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }
    }

    private static final class RejectingExecutor extends AbstractExecutorService {
        private final AtomicInteger attempts = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public void execute(Runnable command) {
            throw new RejectedExecutionException("rejected-" + attempts.incrementAndGet());
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }
    }

    private static final class ManualScheduler extends Scheduler {
        private final Deque<Holder> tasks = new ArrayDeque<>();
        private volatile boolean closed;

        @Override
        public synchronized Cancellable scheduleOnce(Duration delay, Runnable runnable, Executor executor) {
            if (closed) throw new IllegalStateException("scheduler is closed");
            if (delay.isZero() || delay.isNegative()) {
                executor.execute(runnable);
                return completed();
            }
            Holder holder = new Holder(runnable, executor);
            tasks.addLast(holder);
            return holder;
        }

        @Override
        protected Cancellable schedule(Duration initialDelay, Duration interval,
                                       Runnable runnable, Executor executor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double maxFrequency() {
            return 0;
        }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            List<Holder> pending = new ArrayList<>(tasks);
            tasks.clear();
            for (Holder holder : pending) {
                if (!holder.cancelled && holder.task instanceof TaskRunOnClose task) task.run();
            }
        }

        synchronized void runNext() {
            Holder holder = tasks.pollFirst();
            if (holder == null || holder.cancelled) return;
            try {
                holder.executor.execute(holder.task);
            } catch (Throwable error) {
                if (holder.executor instanceof ExecutionContext context) context.reportFailure(error);
                else throw error;
            }
        }

        synchronized int pendingCount() {
            return tasks.size();
        }

        private static Cancellable completed() {
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

        private final class Holder implements Cancellable {
            private final Runnable task;
            private final Executor executor;
            private boolean cancelled;

            private Holder(Runnable task, Executor executor) {
                this.task = task;
                this.executor = executor;
            }

            @Override
            public boolean cancel() {
                if (cancelled) return false;
                cancelled = true;
                tasks.remove(this);
                return true;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }
        }
    }

    public static final class RecordingScheduler extends LightArrayRevolverScheduler {
        private static final AtomicReference<RecordingScheduler> LAST = new AtomicReference<>();
        private final AtomicInteger scheduledCloseTasks = new AtomicInteger();
        private final AtomicInteger closeTaskRuns = new AtomicInteger();
        private volatile boolean closed;
        private volatile boolean closing;

        public RecordingScheduler(Config config, LoggingAdapter log, ThreadFactory threadFactory) {
            super(config, log, threadFactory);
            LAST.set(this);
        }

        @Override
        public Cancellable scheduleOnce(Duration delay, Runnable runnable, Executor executor) {
            if (!(runnable instanceof TaskRunOnClose)) return super.scheduleOnce(delay, runnable, executor);
            TaskRunOnClose tracked = () -> {
                if (closing) closeTaskRuns.incrementAndGet();
                runnable.run();
            };
            Cancellable cancellable = super.scheduleOnce(delay, tracked, executor);
            scheduledCloseTasks.incrementAndGet();
            return cancellable;
        }

        @Override
        public void close() {
            closing = true;
            closed = true;
            super.close();
        }
    }

    static final class CaptureActorRef extends MinimalActorRef {
        private final ActorPath path = new ActorPath("tapio://dispatcher-lifecycle/capture");
        private final BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        private final Consumer<Object> consumer;

        private CaptureActorRef() {
            this(message -> {});
        }

        CaptureActorRef(BlockingQueue<Object> events) {
            this(events::add);
        }

        private CaptureActorRef(Consumer<Object> consumer) {
            this.consumer = consumer;
        }

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
            consumer.accept(message);
            events.add(message);
        }
    }

    private record TestActor(LocalActorRef ref, CaptureActorRef sender, CountDownLatch ready, CountDownLatch stopped) {
        private Object request() throws InterruptedException {
            ref.tell("ping", sender);
            return sender.events.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private record WatchRequest(ActorRef actor) {
    }

    public static final class WatcherProbe extends AbstractActor {
        private final BlockingQueue<ActorRef> terminated;

        public WatcherProbe(BlockingQueue<ActorRef> terminated) {
            this.terminated = terminated;
        }

        @Override
        public AbstractActor.Receive createReceive() {
            return receiveBuilder()
                    .match(WatchRequest.class, request -> {
                        getContext().watch(request.actor());
                        getSender().tell("watching", getSelf());
                    })
                    .match(Terminated.class, notification -> terminated.add(notification.actor()))
                    .build();
        }
    }

    public static final class LifecycleActor extends AbstractActor {
        private final CountDownLatch ready;
        private final CountDownLatch stopped;

        public LifecycleActor(CountDownLatch ready, CountDownLatch stopped) {
            this.ready = ready;
            this.stopped = stopped;
        }

        @Override
        public void preStart() {
            ready.countDown();
        }

        @Override
        public void postStop() {
            stopped.countDown();
        }

        @Override
        public AbstractActor.Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> getSender().tell("pong", getSelf()))
                    .build();
        }
    }
}
