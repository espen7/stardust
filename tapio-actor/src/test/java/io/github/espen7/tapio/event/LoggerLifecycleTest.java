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
package io.github.espen7.tapio.event;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.Mailboxes;
import io.github.espen7.tapio.dispatch.MessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMailbox;
import io.github.espen7.tapio.event.slf4j.Slf4jLogger;
import io.github.espen7.tapio.pattern.AskTimeoutException;
import io.github.espen7.tapio.util.NoStackTrace;
import io.github.espen7.tapio.util.NonFatal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class LoggerLifecycleTest {
    private static final Duration WAIT = Duration.ofSeconds(5);
    private static final BlockingQueue<LogEvent> loggerEvents = new LinkedBlockingQueue<>();
    private static final BlockingQueue<Object> startupWarnings = new LinkedBlockingQueue<>();
    private static final BlockingQueue<ActorRef> recordingRefs = new LinkedBlockingQueue<>();
    private static final ActorRef startupWarningObserver =
            observer("startup-warning-observer", startupWarnings);
    private static volatile CountDownLatch initialized = new CountDownLatch(1);
    private static volatile CountDownLatch stopped = new CountDownLatch(1);
    private static volatile CountDownLatch silentStopped = new CountDownLatch(1);
    private static volatile CountDownLatch initializationBlocked = new CountDownLatch(1);
    private static volatile CountDownLatch releaseInitialization = new CountDownLatch(1);
    private static volatile CountDownLatch blockingStopped = new CountDownLatch(1);
    private static final AtomicInteger recordingCreations = new AtomicInteger();
    private static volatile ActorSystem loggerSystem;
    private static volatile boolean publishOnStop;
    private static volatile ActorRef loggerRef;
    private static volatile boolean customTimestamp;

    private ActorSystem system;

    @BeforeEach
    void resetLoggers() {
        loggerEvents.clear();
        startupWarnings.clear();
        recordingRefs.clear();
        initialized = new CountDownLatch(1);
        stopped = new CountDownLatch(1);
        silentStopped = new CountDownLatch(1);
        initializationBlocked = new CountDownLatch(1);
        releaseInitialization = new CountDownLatch(1);
        blockingStopped = new CountDownLatch(1);
        recordingCreations.set(0);
        loggerSystem = null;
        publishOnStop = false;
        loggerRef = null;
        customTimestamp = false;
    }

    @AfterEach
    void terminateSystem() throws Exception {
        if (system != null) {
            if (!system.getWhenTerminated().toCompletableFuture().isDone()) {
                system.terminate();
            }
            system.getWhenTerminated().toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void configuredLoggerInitializesFiltersAndUnsubscribesBeforeStopping() throws Exception {
        Config config = config(RecordingLogger.class).withFallback(ConfigFactory.parseString("""
                tapio.loglevel = INFO
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """));
        system = ActorSystem.create("logger-lifecycle", config);
        loggerSystem = system;
        loggerRef = RecordingLogger.ref;
        BlockingQueue<Object> published = new LinkedBlockingQueue<>();
        BlockingQueue<Object> deadLetters = new LinkedBlockingQueue<>();
        ActorRef observer = observer("logger-observer", published);
        system.eventStream().subscribe(observer, Info.class);
        system.eventStream().subscribe(observer, DeadLetter.class);

        assertTrue(initialized.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals("OFF", system.settings().StdoutLogLevel);
        assertEquals(LogEvent.Level.INFO, system.eventStream().logLevel());
        system.eventStream().publish(new Info("test", LoggerLifecycleTest.class, "before-level-change"));
        assertEquals("before-level-change", awaitMessage(loggerEvents,
                event -> event.message().equals("before-level-change")).message());

        system.eventStream().setLogLevel(LogEvent.Level.ERROR);
        system.eventStream().publish(new Info("test", LoggerLifecycleTest.class, "filtered-info"));
        assertNull(loggerEvents.poll(100, TimeUnit.MILLISECONDS));
        assertFalse(system.eventStream().isEnabled(LogEvent.Level.INFO));

        system.eventStream().setLogLevel(LogEvent.Level.DEBUG);
        system.eventStream().publish(new Debug("test", LoggerLifecycleTest.class, "after-level-change"));
        assertEquals("after-level-change", await(loggerEvents).message());
        system.eventStream().setLogLevel(LogEvent.Level.OFF);
        assertFalse(system.eventStream().isEnabled(LogEvent.Level.ERROR));
        loggerEvents.clear();
        published.clear();

        publishOnStop = true;
        system.terminate().toCompletableFuture().get(WAIT.toMillis(), TimeUnit.MILLISECONDS);

        assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertLoggedSystemTerminated();
        assertTrue(((InternalActorRef) loggerRef).isTerminated());
        assertEquals("logger-post-stop", ((Info) await(published)).message());
        assertTrue(loggerEvents.isEmpty(), "停止中的 logger 不应再收到自己的 postStop 日志");
        for (Object message : deadLetters) {
            if (message instanceof DeadLetter deadLetter) {
                assertNotEquals(loggerRef.path(), deadLetter.recipient().path());
            }
        }
        system.eventStream().publish(new Info("test", LoggerLifecycleTest.class, "after-stop"));
        assertEquals("after-stop", ((Info) await(published)).message());
        assertTrue(loggerEvents.isEmpty());
    }

    @Test
    void duplicateLoggerClassesReceiveDistinctMonotonicNames() throws Exception {
        system = ActorSystem.create("logger-duplicate", ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger",
                                 "io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger"]
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """));
        loggerSystem = system;

        ActorRef first = recordingRefs.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        ActorRef second = recordingRefs.poll(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals("log1-LoggerLifecycleTest$RecordingLogger", first.path().name());
        assertEquals("log2-LoggerLifecycleTest$RecordingLogger", second.path().name());
    }

    @Test
    void loggerSettingsAreImmutable() {
        system = ActorSystem.create("logger-settings", config(RecordingLogger.class));

        assertThrows(UnsupportedOperationException.class,
                () -> system.settings().Loggers.add(RecordingLogger.class.getName()));
    }

    @Test
    void nonFatalAndNoStackTraceFollowPekkoBoundaries() {
        assertTrue(NonFatal.apply(new AssertionError("non-fatal")));
        assertFalse(NonFatal.apply(new VirtualMachineError() {
        }));
        assertFalse(NonFatal.apply(new LinkageError("fatal")));
        assertFalse(NonFatal.apply(new InterruptedException("interrupted")));

        class ShortException extends RuntimeException implements NoStackTrace {
        }
        ShortException cause = new ShortException();
        assertEquals(" (" + cause.getClass().getName() + ": null)", Logging.stackTraceFor(cause));
        assertEquals("", Logging.stackTraceFor(null));
    }

    @Test
    void stdoutTimestampCanBeOverridden() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        StdOutLogger logger = new StdOutLogger() {
            @Override
            public String timestamp(LogEvent event) {
                return "custom-timestamp";
            }
        };
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            logger.info(new Info("test", LoggerLifecycleTest.class, "message"));
            capture.flush();
        } finally {
            System.setOut(originalOut);
        }
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("[custom-timestamp]"));
    }

    @Test
    void slf4jTimestampCanBeOverridden() throws Exception {
        system = ActorSystem.create("logger-timestamp", ConfigFactory.parseString("""
                tapio.loggers = []
                tapio.stdout-loglevel = OFF
                """));
        customTimestamp = true;
        ActorRef actor = system.actorOf(Props.create(MdcProbe.class), "mdc-probe");
        BlockingQueue<MdcProbe.Result> results = new LinkedBlockingQueue<>();
        actor.tell(new MdcProbe.Request(new Info("test", LoggerLifecycleTest.class, "message"),
                results, false), ActorRef.noSender);

        MdcProbe.Result result = await(results);
        assertEquals("custom-timestamp", result.mdc().get("pekkoTimestamp"));
    }

    @Test
    void laterLoggerFailureRollsBackAlreadyStartedLoggers() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger",
                                 "io.github.espen7.tapio.event.LoggerLifecycleTest$FailingLogger"]
                tapio.stdout-loglevel = ERROR
                tapio.log-dead-letters = off
                """);

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("logger-rollback", config));

        assertEquals("Could not start logger due to [" + error.getCause() + "]", error.getMessage());
        assertInstanceOf(LoggerInitializationException.class, error.getCause());
        assertEquals(1, recordingCreations.get());
        assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertLoggedSystemTerminated();
    }

    @Test
    void allLoggerClassesAreResolvedBeforeAnyLoggerIsCreated() {
        Config config = ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger",
                                 "io.github.espen7.tapio.event.MissingLogger"]
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """);

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("logger-class-loading", config));

        assertEquals(0, recordingCreations.get());
        assertTrue(error.getMessage().contains("Logger specified in config can't be loaded"));
    }

    @Test
    void initializationTimeoutStopsSilentLoggerAndReportsWarning() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger",
                                 "io.github.espen7.tapio.event.LoggerLifecycleTest$SilentLogger"]
                tapio.logger-startup-timeout = 100ms
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """);

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("logger-timeout", config));

        assertInstanceOf(LoggerInitializationException.class, error.getCause());
        Warning warning = (Warning) awaitMessage(startupWarnings, event -> event instanceof Warning);
        assertEquals("EventStream(tapio://logger-timeout)", warning.source());
        assertTrue(warning.message().contains("did not respond within Timeout(100 milliseconds) to InitializeLogger(bus)"));
        assertTrue(silentStopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertLoggedSystemTerminated();
    }

    @Test
    void askTimeoutFailureIsReportedAsLoggerInitializationTimeout() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger",
                                 "io.github.espen7.tapio.event.LoggerLifecycleTest$TimeoutFailureLogger"]
                tapio.logger-startup-timeout = 1s
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """);

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("logger-ask-timeout", config));

        assertInstanceOf(LoggerInitializationException.class, error.getCause());
        Warning warning = (Warning) awaitMessage(startupWarnings, event -> event instanceof Warning);
        assertTrue(warning.message().contains("did not respond within Timeout(1000 milliseconds)"));
        assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertLoggedSystemTerminated();
    }

    @Test
    void blockedLoggerInitializationDoesNotBlockActorSystemCreationIndefinitely() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$BlockingLogger"]
                tapio.logger-startup-timeout = 100ms
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """);

        try {
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                ConfigurationException error = assertThrows(ConfigurationException.class,
                        () -> ActorSystem.create("logger-blocked", config));
                assertInstanceOf(LoggerInitializationException.class, error.getCause());
            });
            assertTrue(initializationBlocked.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        } finally {
            releaseInitialization.countDown();
        }
        assertTrue(blockingStopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
        assertLoggedSystemTerminated();
    }

    @Test
    void defaultLoggerMailboxDrainsRemainingEventsToStdout() throws Exception {
        system = ActorSystem.create("logger-mailbox", ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger"]
                tapio.loglevel = WARNING
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """));
        Mailboxes mailboxes = new Mailboxes(system.settings(), system.eventStream(), system.deadLetters());
        assertEquals(LoggerMessageQueueSemantics.class, mailboxes.getRequiredType(DefaultLogger.class));
        ActorRef owner = observer("logger-mailbox-owner", new LinkedBlockingQueue<>());
        MessageQueue queue = mailboxes.getMailboxType(Props.create(DefaultLogger.class),
                system.dispatchers().config(Dispatchers.DefaultDispatcherId)).create(owner, system);
        assertInstanceOf(LoggerMailbox.class, queue);
        queue.enqueue(owner, Envelope.apply(
                new Info("test", LoggerLifecycleTest.class, "filtered-info"), system.deadLetters(), system));
        queue.enqueue(owner, Envelope.apply(
                new Error("test", LoggerLifecycleTest.class, "drained-error"), system.deadLetters(), system));
        queue.enqueue(owner, Envelope.apply("ignored", system.deadLetters(), system));
        queue.enqueue(owner, Envelope.apply(
                new Warning("test", LoggerLifecycleTest.class, "drained-warning", Map.of("requestId", "r-1"), null),
                system.deadLetters(), system));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        MessageQueue deadLetters = new UnboundedMailbox.MessageQueue();
        try (PrintStream capture = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            System.setOut(capture);
            queue.cleanUp(owner, deadLetters);
            capture.flush();
        } finally {
            System.setOut(originalOut);
        }

        String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("[ERROR]"));
        assertTrue(text.contains("drained-error"));
        assertTrue(text.contains("[WARN]"));
        assertTrue(text.contains("drained-warning"));
        assertTrue(text.contains("[requestId:r-1]"));
        assertFalse(text.contains("filtered-info"));
        assertFalse(text.contains("ignored"));
        assertFalse(deadLetters.hasMessages());
    }

    @Test
    void slf4jLoggerRequiresLoggerMailbox() {
        system = ActorSystem.create("slf4j-mailbox", config(RecordingLogger.class));
        Mailboxes mailboxes = new Mailboxes(system.settings(), system.eventStream(), system.deadLetters());

        assertEquals(LoggerMessageQueueSemantics.class, mailboxes.getRequiredType(Slf4jLogger.class));
        MessageQueue queue = mailboxes.getMailboxType(Props.create(Slf4jLogger.class),
                system.dispatchers().config(Dispatchers.DefaultDispatcherId)).create(system.deadLetters(), system);
        assertInstanceOf(LoggerMailbox.class, queue);
    }

    @Test
    void slf4jLoggerPopulatesStandardAndEventMdcAndClearsIt() throws Exception {
        system = ActorSystem.create("logger-mdc", ConfigFactory.parseString("""
                tapio.loggers = []
                tapio.stdout-loglevel = OFF
                """));
        Map<String, Object> eventMdc = new HashMap<>();
        eventMdc.put("requestId", "r-1");
        eventMdc.put("sourceThread", "event-thread");
        LogEvent event = new Info("test-source", LoggerLifecycleTest.class, "message", eventMdc,
                new LogMarker("test-marker", Map.of("markerField", "marker-value")));
        BlockingQueue<MdcProbe.Result> results = new LinkedBlockingQueue<>();
        system.actorOf(Props.create(MdcProbe.class), "mdc-probe").tell(
                new MdcProbe.Request(event, results, false), ActorRef.noSender);

        MdcProbe.Result result = await(results);
        Map<String, String> mdc = result.mdc();
        assertEquals("test-source", mdc.get("pekkoSource"));
        assertEquals("event-thread", mdc.get("sourceThread"));
        assertTrue(mdc.get("pekkoTimestamp").matches("\\d{2}:\\d{2}:\\d{2}\\.\\d{3}UTC"));
        assertEquals("test-system", mdc.get("sourceActorSystem"));
        assertEquals("tapio://test-system", mdc.get("pekkoAddress"));
        assertEquals("42", mdc.get("pekkoUid"));
        assertEquals("r-1", mdc.get("requestId"));
        assertEquals("marker-value", mdc.get("markerField"));
        assertTrue(result.cleared());
    }

    @Test
    void loggingAdapterAttachesMdcToEvents() throws Exception {
        system = ActorSystem.create("logger-adapter-mdc", config(RecordingLogger.class));
        LoggingAdapter log = Logging.getLogger(system.eventStream(), "test-source", LoggerLifecycleTest.class);
        log.mdc(Map.of("requestId", "r-2"));

        log.info("with-mdc");

        LogEvent event = awaitMessage(loggerEvents, value -> value.message().equals("with-mdc"));
        assertEquals("r-2", event.mdc().get("requestId"));
        assertThrows(UnsupportedOperationException.class, () -> event.mdc().put("other", "value"));
    }

    @Test
    void loggingAdapterUsesPekkoTemplateFormatting() throws Exception {
        system = ActorSystem.create("logger-adapter-format", config(RecordingLogger.class));
        LoggingAdapter log = Logging.getLogger(system.eventStream(), "test-source", LoggerLifecycleTest.class);
        RuntimeException parameter = new IllegalStateException("parameter-failure");

        log.info("failed {}", parameter);
        LogEvent parameterEvent = awaitMessage(loggerEvents,
                value -> value.message().startsWith("failed "));
        assertEquals("failed " + parameter, parameterEvent.message());
        assertNull(parameterEvent.cause());

        log.info("no-placeholders", "first", "second");
        assertEquals("no-placeholders WARNING arguments left: 2",
                awaitMessage(loggerEvents, value -> value.message().startsWith("no-placeholders")).message());

        log.info("{} {} {}", new int[]{1, 2, 3});
        assertEquals("1 2 3", await(loggerEvents).message());

        Object[] referenceArray = {"first", "second"};
        log.info("{} {}", (Object) referenceArray);
        assertEquals("first second", await(loggerEvents).message());

        log.info("{}", (Object) null);
        assertEquals("null", await(loggerEvents).message());

        RuntimeException cause = new IllegalStateException("explicit-failure");
        log.error(cause, "failed {}", "explicit");
        LogEvent causeEvent = awaitMessage(loggerEvents,
                value -> value.message().equals("failed explicit"));
        assertSame(cause, causeEvent.cause());
    }

    @Test
    void slf4jLoggerClearsMdcWhenPublishingThrows() throws Exception {
        system = ActorSystem.create("logger-mdc-failure", ConfigFactory.parseString("""
                tapio.loggers = []
                tapio.stdout-loglevel = OFF
                """));
        LogEvent event = new Info("test-source", LoggerLifecycleTest.class, "message");
        BlockingQueue<MdcProbe.Result> results = new LinkedBlockingQueue<>();
        system.actorOf(Props.create(MdcProbe.class), "mdc-probe").tell(
                new MdcProbe.Request(event, results, true), ActorRef.noSender);

        MdcProbe.Result result = await(results);
        assertInstanceOf(IllegalStateException.class, result.failure());
        assertEquals("publish-failure", result.failure().getMessage());
        assertTrue(result.cleared());
    }

    @Test
    void unhandledMessagesAreForwardedWhenDebugUnhandledIsEnabled() throws Exception {
        system = ActorSystem.create("logger-unhandled", ConfigFactory.parseString("""
                tapio.loggers = ["io.github.espen7.tapio.event.LoggerLifecycleTest$RecordingLogger"]
                tapio.loglevel = DEBUG
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                tapio.actor.debug.unhandled = on
                """));
        loggerSystem = system;

        system.actorOf(Props.create(UnhandledActor.class), "unhandled").tell("probe", ActorRef.noSender);

        String message = awaitMessage(loggerEvents,
                event -> event.message().startsWith("unhandled message from ")).message();
        assertTrue(message.endsWith(": probe"));
    }

    private static void assertLoggedSystemTerminated() throws Exception {
        assertNotNull(loggerSystem);
        assertTrue(loggerSystem.getWhenTerminated().toCompletableFuture()
                .get(WAIT.toMillis(), TimeUnit.MILLISECONDS) != null);
    }

    private static Config config(Class<? extends AbstractActor> loggerClass) {
        return ConfigFactory.parseString("tapio.loggers = [\"" + loggerClass.getName() + "\"]");
    }

    private static ActorRef observer(String name, BlockingQueue<Object> events) {
        return new ActorRef() {
            private final ActorPath path = new ActorPath("tapio://logger-tests/" + name);

            @Override
            public ActorPath path() {
                return path;
            }

            @Override
            public void tell(Object message, ActorRef sender) {
                events.add(message);
            }
        };
    }

    private static <T> T await(BlockingQueue<T> queue) throws InterruptedException {
        return awaitMessage(queue, ignored -> true);
    }

    private static <T> T awaitMessage(BlockingQueue<T> queue, Predicate<T> predicate) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                fail("expected event was not delivered");
                return null;
            }
            T value = queue.poll(remaining, TimeUnit.NANOSECONDS);
            if (value != null && predicate.test(value)) return value;
        }
    }

    public static final class MdcProbe extends Slf4jLogger {
        @Override
        public AbstractActor.Receive createReceive() {
            return receiveBuilder()
                    .match(Request.class, request -> {
                        Map<String, String> captured = new HashMap<>();
                        boolean[] cleared = {false};
                        Throwable failure = null;
                        try {
                            withMdc(request.event(), "test-source", "test-system", "tapio://test-system", "42",
                                    captured::put, () -> cleared[0] = true, () -> {
                                        if (request.fail()) throw new IllegalStateException("publish-failure");
                                    });
                        } catch (RuntimeException error) {
                            failure = error;
                        }
                        request.results().add(new Result(Map.copyOf(captured), cleared[0], failure));
                    })
                    .build();
        }

        @Override
        protected String formatTimestamp(long timestamp) {
            return customTimestamp ? "custom-timestamp" : super.formatTimestamp(timestamp);
        }

        private record Request(LogEvent event, BlockingQueue<Result> results, boolean fail) {
        }

        private record Result(Map<String, String> mdc, boolean cleared, Throwable failure) {
        }
    }

    public static final class RecordingLogger extends AbstractActor {
        private static volatile ActorRef ref;

        public RecordingLogger() {
            recordingCreations.incrementAndGet();
        }

        @Override
        public void preStart() {
            ref = getSelf();
            recordingRefs.add(ref);
            loggerSystem = getContext().system();
            loggerSystem.eventStream().subscribe(startupWarningObserver, Warning.class);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(InitializeLogger.class, ignored -> {
                        initialized.countDown();
                        getSender().tell(LoggerInitialized.getInstance(), getSelf());
                    })
                    .match(LogEvent.class, loggerEvents::add)
                    .build();
        }

        @Override
        public void postStop() {
            stopped.countDown();
            if (publishOnStop && loggerSystem != null) {
                loggerSystem.eventStream().publish(new Info("logger", RecordingLogger.class, "logger-post-stop"));
            }
        }
    }

    public static final class FailingLogger extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(InitializeLogger.class,
                    ignored -> getSender().tell("not-initialized", getSelf())).build();
        }
    }

    public static final class SilentLogger extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(InitializeLogger.class, ignored -> {
            }).build();
        }

        @Override
        public void postStop() {
            silentStopped.countDown();
        }
    }

    public static final class TimeoutFailureLogger extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(InitializeLogger.class, ignored -> getSender().tell(
                    new io.github.espen7.tapio.actor.Status.Failure(new AskTimeoutException("ask timed out")), getSelf())).build();
        }
    }

    public static final class BlockingLogger extends AbstractActor {
        @Override
        public void preStart() {
            loggerSystem = getContext().system();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(InitializeLogger.class, ignored -> {
                initializationBlocked.countDown();
                try {
                    releaseInitialization.await();
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                }
            }).build();
        }

        @Override
        public void postStop() {
            blockingStopped.countDown();
        }
    }

    public static final class UnhandledActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
    }
}
