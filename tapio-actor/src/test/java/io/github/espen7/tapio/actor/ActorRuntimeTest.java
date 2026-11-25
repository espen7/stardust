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
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.dispatch.Dispatcher;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.event.Info;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.pattern.AskTimeoutException;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ActorRuntimeTest {
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;

    @Test
    void settingsAndDefaultDispatcherUseApplicationConfig() throws Exception {
        Config config = ConfigFactory.parseString("""
                pekko.actor.default-dispatcher.throughput = 11
                pekko.loglevel = "WARNING"
                pekko.actor.loglevel = "DEBUG"
                pekko.log-dead-letters = off
                """);
        system = ActorSystem.create("configured-dispatcher", config);

        assertEquals("configured-dispatcher", system.settings().name());
        assertEquals(0, system.settings().LogDeadLetters);
        assertEquals(LogEvent.Level.WARNING, system.eventStream().logLevel());
        assertSame(system.dispatcher(), system.dispatchers().defaultGlobalDispatcher());
        assertEquals(11, system.dispatcher().throughput());
        assertEquals("reply", ask(system.actorOf(Props.create(EchoActor.class), "echo"), "reply"));
    }

    @Test
    void settingsRetainLegacyLogLevelAsFallback() {
        system = ActorSystem.create("legacy-loglevel", ConfigFactory.parseString("pekko.actor.loglevel = DEBUG"));

        assertEquals(LogEvent.Level.DEBUG, system.eventStream().logLevel());
        assertEquals(5, system.dispatcher().throughput());
    }

    @Test
    void settingsRejectConflictingShutdownConfig() {
        Config config = ConfigFactory.parseString("pekko.coordinated-shutdown.terminate-actor-system = off");

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("invalid-shutdown", config));
        assertTrue(error.getMessage().contains("run-by-actor-system-terminate=on"));
    }

    @Test
    void dispatchersLookupResolvesAliasesAndInheritsDefaultConfig() {
        Config config = ConfigFactory.parseString("""
                pekko.actor.default-dispatcher.throughput = 9
                dispatchers.shared {}
                dispatchers.alias = "dispatchers.shared"
                dispatchers.next = "dispatchers.alias"
                dispatchers.loop = "dispatchers.loop"
                dispatchers.invalid = 1
                """);
        system = ActorSystem.create("dispatcher-lookup", config);
        Dispatchers dispatchers = system.dispatchers();

        assertTrue(dispatchers.hasDispatcher("dispatchers.alias"));
        assertFalse(dispatchers.hasDispatcher("dispatchers.missing"));
        Dispatcher shared = dispatchers.lookup("dispatchers.next");
        assertEquals(9, shared.throughput());
        assertNotSame(system.dispatcher(), shared);
        assertSame(shared, dispatchers.lookup("dispatchers.shared"));
        assertSame(shared, dispatchers.lookup("dispatchers.alias"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.missing"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.loop"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.invalid"));
        assertSame(system.dispatcher(), dispatchers.lookup(Dispatchers.DefaultDispatcherId));
        system.close();
        assertThrows(IllegalStateException.class, () -> dispatchers.lookup("dispatchers.shared"));
    }

    @Test
    void actorSystemNameUsesPekkoNameRules() {
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("-leading"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("with space"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("with/slash"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create(""));

        ActorSystem valid = ActorSystem.create("a-system_2");
        assertDoesNotThrow(() -> valid.terminate().toCompletableFuture().join());
    }

    @Test
    void systemActorOfUsesSystemGuardianPath() throws Exception {
        system = ActorSystem.create("system-actors");
        ActorRef actor = system.systemActorOf(Props.create(EchoActor.class), "service");

        assertEquals("/system/service", actor.path().toStringWithoutAddress());
        assertEquals("service", actor.path().name());
        assertEquals("reply", ask(actor, "reply"));
    }

    @Test
    void coordinatedShutdownRunsRegisteredTaskBeforeTermination() throws Exception {
        system = ActorSystem.create("coordinated-shutdown");
        BlockingQueue<Object> completed = new LinkedBlockingQueue<>();
        system.coordinatedShutdown().addTask(
                CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
                "test-task",
                () -> {
                    completed.add("done");
                    return CompletableFuture.completedFuture(Done.done());
                });

        system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals("done", completed.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(CoordinatedShutdown.ActorSystemTerminateReason.class,
                system.coordinatedShutdown().shutdownReason().orElseThrow().getClass());
    }

    @Test
    void topLevelActorOfAttachesChildToUserGuardian() throws Exception {
        system = ActorSystem.create("attach-child");
        ActorRef actor = system.actorOf(Props.create(EchoActor.class), "echo");

        assertEquals("/user/echo", actor.path().toStringWithoutAddress());
        assertEquals("echo", actor.path().name());
        assertEquals("hello", ask(actor, "hello"));
    }

    @AfterEach
    void stopActorSystem() throws Exception {
        if (system != null) system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void askDeliversMessageAndReturnsReply() throws Exception {
        system = ActorSystem.create("ask-reply");
        ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");

        Object response = Patterns.ask(echo, "hello", Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals("hello", response);
    }

    @Test
    void identifyRepliesForLiveAndDeadLetterReferences() throws Exception {
        system = ActorSystem.create("identify");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "actor");
        ActorIdentity live = (ActorIdentity) ask(actor, new Identify("live"));
        assertEquals("live", live.correlationId());
        assertEquals(actor, live.getActorRef().orElseThrow());

        BlockingQueue<Object> replies = new LinkedBlockingQueue<>();
        ActorRef probe = system.actorOf(Props.create(EventSubscriberActor.class, replies), "probe");
        system.deadLetters().tell(new Identify("missing"), probe);
        ActorIdentity missing = (ActorIdentity) replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(missing);
        assertEquals("missing", missing.correlationId());
        assertTrue(missing.getActorRef().isEmpty());
    }

    @Test
    void schedulerReportsSubmissionFailureToExecutionContext() throws Exception {
        system = ActorSystem.create("scheduler-report");
        BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();
        RuntimeException rejection = new RuntimeException("rejected");
        io.github.espen7.tapio.dispatch.ExecutionContext executionContext =
                new io.github.espen7.tapio.dispatch.ExecutionContext() {
                    @Override
                    public void execute(Runnable runnable) {
                        throw rejection;
                    }

                    @Override
                    public void reportFailure(Throwable cause) {
                        failures.add(cause);
                    }
                };
        system.scheduler().scheduleOnce(Duration.ofMillis(10), () -> fail("must not execute"), executionContext);
        assertSame(rejection, failures.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void deadLetterListenerPublishesInfoWithRecipientSource() throws Exception {
        system = ActorSystem.create("dead-letter-info");
        BlockingQueue<Object> logs = new LinkedBlockingQueue<>();
        ActorRef probe = system.actorOf(Props.create(EventSubscriberActor.class, logs), "log-probe");
        system.eventStream().subscribe(probe, Info.class);
        ActorRef recipient = system.actorOf(Props.create(SilentActor.class), "recipient");
        ActorRef listener = system.actorOf(Props.create(io.github.espen7.tapio.event.DeadLetterListener.class,
                1, null, "infinite"), "test-listener");
        listener.tell(new DeadLetter("missing", system.deadLetters(), recipient));

        Info info = (Info) logs.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(info);
        assertEquals(recipient.path().toString(), info.source());
        assertEquals(recipient.getClass(), info.sourceClass());
        assertTrue(info.message().contains("Message [java.lang.String] to " + recipient + " was not delivered."));
        assertTrue(info.message().contains("[1] dead letters encountered, no more dead letters will be logged."));
        assertNotNull(info.marker());
        assertEquals("pekkoDeadLetter", info.marker().name());
        assertEquals("java.lang.String", info.marker().properties().get("pekkoMessageClass"));
    }

    @Test
    void actorLogPublishesStructuredEventWithPekkoMessageTemplate() throws Exception {
        system = ActorSystem.create("logging");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "event-subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
        ActorRef actor = system.actorOf(Props.create(LoggingActor.class), "logger");

        assertEquals("logged", ask(actor, "request-1"));

        Info event = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (event == null && System.nanoTime() < deadline) {
            Object next = events.poll(100, TimeUnit.MILLISECONDS);
            if (next instanceof Info info) event = info;
        }
        assertNotNull(event);
        assertEquals("pekko://logging/user/logger", event.source());
        assertEquals(LoggingActor.class, event.sourceClass());
        assertEquals("handled request-1", event.message());
        assertEquals(LogEvent.Level.INFO, event.level());
    }

    @Test
    void eventStreamUpdatesCachedClassificationsAfterSubscriptionChanges() throws Exception {
        system = ActorSystem.create("event-stream-cache");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        Info event = new Info("test", ActorRuntimeTest.class, "cached");

        system.eventStream().publish(event);
        assertTrue(system.eventStream().subscribe(subscriber, LogEvent.class));
        system.eventStream().publish(event);
        assertEquals(event, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));

        assertTrue(system.eventStream().unsubscribe(subscriber, LogEvent.class));
        system.eventStream().publish(event);
        assertEquals(null, events.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void receiveBuilderMatchesTypeAndUsesFallback() throws Exception {
        system = ActorSystem.create("receive-builder");
        ActorRef actor = system.actorOf(Props.create(ReceiveOrderActor.class), "receiver");

        assertEquals("string", ask(actor, "value"));
        assertEquals("fallback", ask(actor, 42));
    }

    @Test
    void actorContextIsAvailableDuringConstruction() throws Exception {
        system = ActorSystem.create("constructor-context");
        ActorRef actor = system.actorOf(Props.create(ConstructorContextActor.class), "actor");

        assertEquals(actor.path().toString(), ask(actor, "self"));
        assertThrows(ActorInitializationException.class, ConstructorContextActor::new);
    }

    @Test
    void propsSelectsConstructorByArgumentType() throws Exception {
        system = ActorSystem.create("props-constructor");
        ActorRef actor = system.actorOf(Props.create(OverloadedActor.class, "selected"), "actor");

        assertEquals("selected", ask(actor, "value"));
    }

    @Test
    void unhandledTerminatedStopsActorUsingPekkoDefaultDecider() throws Exception {
        system = ActorSystem.create("death-pact");
        AtomicInteger instances = new AtomicInteger();
        ActorRef watched = system.actorOf(Props.create(SilentActor.class), "watched");
        ActorRef watcher = system.actorOf(Props.create(DeathPactActor.class, instances), "watcher");

        assertEquals("watching", ask(watcher, new Watch(watched)));
        watched.stop();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!watcher.isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(watcher.isTerminated());
    }

    @Test
    void defaultDeciderMatchesPekkoExceptionClassification() {
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new ActorInitializationException("initialize")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new ActorKilledException("kill")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new DeathPactException(null)));
        assertEquals(SupervisorStrategy.Directive.Restart,
                SupervisorStrategy.defaultDecider.apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Escalate,
                SupervisorStrategy.defaultDecider.apply(new AssertionError("fatal")));
    }

    @Test
    void childRestartStatsEnforceRetryCountsAndWindowReset() throws Exception {
        system = ActorSystem.create("restart-stats");
        ActorRef child = system.actorOf(Props.create(SilentActor.class), "child");

        io.github.espen7.tapio.actor.dungeon.ChildRestartStats limited =
                new io.github.espen7.tapio.actor.dungeon.ChildRestartStats(child);
        assertTrue(limited.requestRestartPermission(2, null));
        assertTrue(limited.requestRestartPermission(2, null));
        assertFalse(limited.requestRestartPermission(2, null));
        assertFalse(new io.github.espen7.tapio.actor.dungeon.ChildRestartStats(child)
                .requestRestartPermission(0, null));
        assertTrue(new io.github.espen7.tapio.actor.dungeon.ChildRestartStats(child)
                .requestRestartPermission(-1, null));

        io.github.espen7.tapio.actor.dungeon.ChildRestartStats windowed =
                new io.github.espen7.tapio.actor.dungeon.ChildRestartStats(child);
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertFalse(windowed.requestRestartPermission(2, 60_000));

        io.github.espen7.tapio.actor.dungeon.ChildRestartStats expired =
                new io.github.espen7.tapio.actor.dungeon.ChildRestartStats(child, 2,
                        System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
        assertTrue(expired.requestRestartPermission(2, 10));
        assertEquals(1, expired.maxNrOfRetriesCount());
        assertNotEquals(0, expired.restartTimeWindowStartNanos());
    }

    @Test
    void allForOneRestartsEverySiblingAndStopsAllWhenLimitIsExceeded() throws Exception {
        system = ActorSystem.create("all-for-one");
        AtomicInteger firstInstances = new AtomicInteger();
        AtomicInteger secondInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(AllForOneParent.class,
                new AllForOneStrategy(1, Duration.ofSeconds(10), ignored -> SupervisorStrategy.Directive.Restart),
                firstInstances, secondInstances), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertEquals(1, (int) ask(children.get(1), "instance"));

        parent.tell("fail-first");
        awaitInstance(children.get(0), 2);
        awaitInstance(children.get(1), 2);

        parent.tell("fail-first");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (children.stream().anyMatch(child -> !child.isTerminated()) && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(children.stream().allMatch(ActorRef::isTerminated));
    }

    @Test
    void oneForOneRetryLimitStopsOnlyTheFailedChild() throws Exception {
        system = ActorSystem.create("one-for-one-limit");
        AtomicInteger firstInstances = new AtomicInteger();
        AtomicInteger secondInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(1, Duration.ofSeconds(10), ignored -> SupervisorStrategy.Directive.Restart),
                firstInstances, secondInstances), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        parent.tell("fail-first");
        awaitInstance(children.get(0), 2);
        assertEquals(1, (int) ask(children.get(1), "instance"));

        parent.tell("fail-first");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!children.get(0).isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(children.get(0).isTerminated());
        assertFalse(children.get(1).isTerminated());
        assertEquals(1, (int) ask(children.get(1), "instance"));
    }

    @Test
    void resumeKeepsTheFailedChildInstance() throws Exception {
        system = ActorSystem.create("resume-directive");
        AtomicInteger firstInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume),
                firstInstances, new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertEquals(1, firstInstances.get());
    }

    @Test
    void escalateFailsAndRestartsTheSupervisor() throws Exception {
        system = ActorSystem.create("escalate-directive");
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Escalate),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        java.util.List<ActorRef> restartedChildren = children;
        while (System.nanoTime() < deadline) {
            if (children.stream().allMatch(ActorRef::isTerminated)) {
                restartedChildren = awaitChildren(parent);
                break;
            }
            Thread.yield();
        }
        assertFalse(parent.isTerminated());
        assertTrue(children.stream().allMatch(ActorRef::isTerminated));
        assertEquals(2, restartedChildren.size());
        assertNotEquals(children.get(0), restartedChildren.get(0));
    }

    @Test
    void strategyLoggingCanBeDisabled() throws Exception {
        system = ActorSystem.create("strategy-logging");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume, false),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertNull(events.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void resumeFailurePublishesWarningWhenStrategyLoggingIsEnabled() throws Exception {
        system = ActorSystem.create("strategy-warning");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        Object event = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertInstanceOf(io.github.espen7.tapio.event.Warning.class, event);
        assertEquals(LogEvent.Level.WARNING, ((LogEvent) event).level());
    }

    private static void awaitInstance(ActorRef actor, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            try {
                if ((int) ask(actor, "instance") == expected) return;
            } catch (ExecutionException ignored) {
                Thread.yield();
            }
        }
        fail("Actor did not reach instance " + expected);
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<ActorRef> awaitChildren(ActorRef parent) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            java.util.List<ActorRef> children = (java.util.List<ActorRef>) ask(parent, "children");
            if (children.size() == 2) return children;
            Thread.yield();
        }
        return fail("Parent did not register both children");
    }

    @Test
    void parentRestartRestartsSurvivingChildren() throws Exception {
        system = ActorSystem.create("restart-survivors");
        AtomicInteger instances = new AtomicInteger();
        CountDownLatch restarted = new CountDownLatch(1);
        ActorRef parent = system.actorOf(Props.create(ParentWithSurvivor.class, instances, restarted), "parent");

        assertEquals("spawned", ask(parent, "spawn"));
        parent.tell("fail");

        assertTrue(restarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void parentWaitsForChildrenBeforeTerminating() throws Exception {
        system = ActorSystem.create("wait-for-children");
        CountDownLatch childPostStop = new CountDownLatch(1);
        CountDownLatch allowChildStop = new CountDownLatch(1);
        ActorRef parent = system.actorOf(
                Props.create(ParentWithBlockingChild.class, childPostStop, allowChildStop), "parent");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");

        assertEquals("watching", ask(watcher, new Watch(parent)));
        assertEquals("spawned", ask(parent, "spawn"));
        parent.stop();

        assertTrue(childPostStop.await(WAIT_SECONDS, TimeUnit.SECONDS));
        assertFalse(parent.isTerminated());
        allowChildStop.countDown();
        assertEquals(parent.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void actorPathWithoutAddressReturnsOnlyPathElements() {
        assertEquals("/user/worker", new ActorPath("pekko://system/user/worker").toStringWithoutAddress());
    }

    @Test
    void becomeAndUnbecomeSwitchAndRestoreBehavior() throws Exception {
        system = ActorSystem.create("behavior-stack");
        ActorRef actor = system.actorOf(Props.create(BehaviorActor.class), "behavior");

        assertEquals("initial", ask(actor, "ping"));
        assertEquals("switched", ask(actor, "switch"));
        assertEquals("initial", ask(actor, "back"));
    }

    @Test
    void askCompletesWithTimeoutWhenActorDoesNotReply() throws Exception {
        system = ActorSystem.create("ask-timeout");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "silent");

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> Patterns.ask(actor, "request", Duration.ofMillis(50))
                        .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));

        assertInstanceOf(AskTimeoutException.class, failure.getCause());
        assertEquals("Ask timed out on [" + actor + "] after [50 ms]. "
                        + "Message of type [java.lang.String]. A typical reason for `AskTimeoutException` is that "
                        + "the recipient actor didn't send a reply.", failure.getCause().getMessage());
    }

    @Test
    void askWithNonPositiveTimeoutReturnsFailedStageWithoutSending() throws Exception {
        system = ActorSystem.create("ask-invalid-timeout");
        BlockingQueue<Object> requests = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(EventSubscriberActor.class, requests), "probe");

        for (Duration timeout : new Duration[] {Duration.ZERO, Duration.ofMillis(-1)}) {
            CompletionStage<Object> result = Patterns.ask(actor, "request", timeout);
            assertTrue(result.toCompletableFuture().isCompletedExceptionally());
            CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
            assertInstanceOf(IllegalArgumentException.class, error.getCause());
        }
        actor.tell("barrier");
        assertEquals("barrier", requests.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void askToUnsupportedRecipientReturnsFailedStage() {
        CompletionStage<Object> result = Patterns.ask(null, 3.14, Duration.ofSeconds(1));

        assertTrue(result.toCompletableFuture().isCompletedExceptionally());
        CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals("Unsupported recipient type, question not sent to [null]. Message of type [java.lang.Double].",
                error.getCause().getMessage());
    }

    @Test
    void askToTerminatedActorFailsImmediatelyAndPublishesDeadLetter() throws Exception {
        system = ActorSystem.create("ask-terminated");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "stopped");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        assertEquals("watching", ask(watcher, new Watch(actor)));
        actor.stop();
        assertEquals(actor.path().toString(), ask(watcher, "await-termination"));
        // DeathWatch 通知先于 Dispatcher 注销，在有上限的等待内确认 Mailbox 已切换。
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        LocalActorRef stopped = (LocalActorRef) actor;
        while (!stopped.underlying().mailbox().isClosed() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(stopped.underlying().mailbox().isClosed());

        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ZERO);
        assertTrue(result.toCompletableFuture().isCompletedExceptionally());
        CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertInstanceOf(AskTimeoutException.class, error.getCause());
        assertEquals("Recipient [" + actor + "] had already been terminated. Message of type [java.lang.String].",
                error.getCause().getMessage());
        assertEquals(new DeadLetter("request", system.deadLetters(), actor), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void askUnwrapsClassicStatusReplies() throws Exception {
        system = ActorSystem.create("ask-status");
        ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");

        assertEquals("accepted", ask(echo, new Status.Success("accepted")));
        IllegalStateException cause = new IllegalStateException("rejected");
        ExecutionException failure = assertThrows(ExecutionException.class, () -> ask(echo, new Status.Failure(cause)));
        assertSame(cause, failure.getCause());
    }

    @Test
    void askReplyRefRegistersNotifiesWatchersAndSendsDuplicateReplyToDeadLetters() throws Exception {
        system = ActorSystem.create("ask-ref");
        BlockingQueue<ActorRef> replies = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(AskSenderProbe.class, replies), "probe");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);
        LocalActorRefProvider provider = ((ActorSystemImpl) system).provider();
        VirtualPathContainer temp = (VirtualPathContainer) provider.tempContainer();

        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ofSeconds(WAIT_SECONDS));
        ActorRef replyTo = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(replyTo);
        ActorPath path = replyTo.path();
        assertTrue(path.toString().startsWith("pekko://ask-ref/temp/probe$"));
        assertSame(replyTo, temp.getChild(path.name()));
        assertEquals("watching", ask(watcher, new Watch(replyTo)));

        replyTo.tell("response", actor);
        assertEquals("response", result.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(path.toString(), ask(watcher, "await-termination"));
        assertTrue(replyTo.isTerminated());
        assertSame(path, replyTo.path());
        assertNull(temp.getChild(path.name()));

        replyTo.tell("duplicate", actor);
        assertEquals(new DeadLetter("duplicate", actor, replyTo), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void timedOutReplyRefStaysUnregisteredWhenPathIsFirstReadAfterStop() throws Exception {
        system = ActorSystem.create("ask-late-reply");
        BlockingQueue<ActorRef> replies = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(AskSenderProbe.class, replies), "probe");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);

        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ofMillis(50));
        ActorRef replyTo = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(replyTo);
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> result.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertInstanceOf(AskTimeoutException.class, error.getCause());
        // 定时器顺序执行，后一项任务运行时 ask 的清理回调已经返回。
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        system.scheduleOnce(Duration.ofNanos(1), () -> barrier.complete(null));
        barrier.get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertTrue(replyTo.isTerminated());
        ActorPath path = replyTo.path();
        VirtualPathContainer temp = (VirtualPathContainer) ((ActorSystemImpl) system).provider().tempContainer();
        assertNull(temp.getChild(path.name()));
        replyTo.tell("late", actor);
        assertEquals(new DeadLetter("late", actor, replyTo), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void schedulerCloseRunsOnlyMarkedPendingTasks() throws Exception {
        system = ActorSystem.create("scheduler-close");
        AtomicInteger calls = new AtomicInteger();
        Scheduler scheduler = system.scheduler();
        Duration delay = Duration.ofHours(1);
        ActorRef quietActor = system.actorOf(Props.create(SilentActor.class), "silent");
        CompletionStage<Object> pendingAsk = Patterns.ask(quietActor, "pending", delay);
        scheduler.scheduleOnce(delay, () -> calls.addAndGet(100));
        scheduler.scheduleOnce(delay, (Scheduler.TaskRunOnClose) calls::incrementAndGet);
        Cancellable cancelled = scheduler.scheduleOnce(delay, (Scheduler.TaskRunOnClose) () -> calls.addAndGet(10));
        assertTrue(cancelled.cancel());
        assertTrue(cancelled.isCancelled());
        assertThrows(IllegalArgumentException.class, () -> scheduler.scheduleOnce(Duration.ofDays(3650), () -> {}));

        system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, calls.get());
        assertFalse(pendingAsk.toCompletableFuture().isDone());
        assertThrows(IllegalStateException.class, () -> scheduler.scheduleOnce(delay, () -> {}));
    }

    public static final class AskSenderProbe extends AbstractActor {
        private final BlockingQueue<ActorRef> replies;

        public AskSenderProbe(BlockingQueue<ActorRef> replies) {
            this.replies = replies;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> replies.add(getSender())).build();
        }
    }

    @Test
    void watcherReceivesTerminatedAfterWatchedActorStops() throws Exception {
        system = ActorSystem.create("death-watch");
        ActorRef watched = system.actorOf(Props.create(SilentActor.class), "watched");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");

        assertEquals("watching", ask(watcher, new Watch(watched)));
        watched.stop();

        assertEquals(watched.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void childFailureRestartsActorInstance() throws Exception {
        system = ActorSystem.create("restart");
        AtomicInteger instances = new AtomicInteger();
        ActorRef child = system.actorOf(Props.create(RestartingActor.class,
                () -> new RestartingActor(instances.incrementAndGet())), "restartable");

        assertEquals(1, ask(child, "instance"));
        child.tell("fail");
        assertEquals(2, ask(child, "instance"));
        assertEquals(2, instances.get());
    }

    @Test
    void terminateCompletesSystemTerminationStage() throws Exception {
        system = ActorSystem.create("terminate");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "actor");
        assertNotNull(actor);

        Terminated terminated = system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertTrue(actor.isTerminated());
        assertTrue(terminated.actor().isTerminated());
        assertTrue(system.getWhenTerminated().toCompletableFuture().isDone());
    }

    private static Object ask(ActorRef actor, Object message) throws Exception {
        return Patterns.ask(actor, message, Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    public static class EchoActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> getSender().tell(message, getSelf())).build();
        }
    }

    public static class LoggingActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                log().info("handled {}", message);
                getSender().tell("logged", getSelf());
            }).build();
        }
    }

    public static class EventSubscriberActor extends AbstractActor {
        private final BlockingQueue<Object> events;

        public EventSubscriberActor(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(events::add).build();
        }
    }

    public static class ConstructorContextActor extends AbstractActor {
        private final String selfPath = getSelf().path().toString();

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, ignored -> getSender().tell(selfPath, getSelf())).build();
        }
    }

    public static class OverloadedActor extends AbstractActor {
        private final String value;

        public OverloadedActor(String value) {
            this.value = value;
        }

        public OverloadedActor(Integer value) {
            this.value = value.toString();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, ignored -> getSender().tell(value, getSelf())).build();
        }
    }

    public static class DeathPactActor extends AbstractActor {
        private final int instance;

        public DeathPactActor(AtomicInteger instances) {
            instance = instances.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Watch.class, request -> {
                        getContext().watch(request.actor());
                        getSender().tell("watching", getSelf());
                    })
                    .match(String.class, message -> {
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class ParentWithSurvivor extends AbstractActor {
        private final AtomicInteger instances;
        private final CountDownLatch restarted;

        public ParentWithSurvivor(AtomicInteger instances, CountDownLatch restarted) {
            this.instances = instances;
            this.restarted = restarted;
        }

        @Override
        public void preRestart(Throwable reason, Object message) throws Exception {
            postStop();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("spawn")) {
                    getContext().actorOf(Props.create(CountedChild.class, instances, restarted), "survivor");
                    getSender().tell("spawned", getSelf());
                } else if (message.equals("fail")) {
                    throw new IllegalStateException("restart parent");
                }
            }).build();
        }
    }

    public static class CountedChild extends AbstractActor {
        public CountedChild(AtomicInteger instances, CountDownLatch restarted) {
            if (instances.incrementAndGet() > 1) restarted.countDown();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    public static class ParentWithBlockingChild extends AbstractActor {
        private final CountDownLatch childPostStop;
        private final CountDownLatch allowChildStop;

        public ParentWithBlockingChild(CountDownLatch childPostStop, CountDownLatch allowChildStop) {
            this.childPostStop = childPostStop;
            this.allowChildStop = allowChildStop;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("spawn")) {
                    getContext().actorOf(Props.create(BlockingStopChild.class, childPostStop, allowChildStop), "child");
                    getSender().tell("spawned", getSelf());
                }
            }).build();
        }
    }

    public static class BlockingStopChild extends AbstractActor {
        private final CountDownLatch childPostStop;
        private final CountDownLatch allowChildStop;

        public BlockingStopChild(CountDownLatch childPostStop, CountDownLatch allowChildStop) {
            this.childPostStop = childPostStop;
            this.allowChildStop = allowChildStop;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }

        @Override
        public void postStop() throws Exception {
            childPostStop.countDown();
            if (!allowChildStop.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test did not release child termination");
            }
        }
    }

    public static class ReceiveOrderActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> getSender().tell("string", getSelf()))
                    .matchAny(message -> getSender().tell("fallback", getSelf()))
                    .build();
        }
    }

    public static class BehaviorActor extends AbstractActor {
        private final Receive switched = receiveBuilder()
                .match(String.class, message -> {
                    if (message.equals("back")) {
                        getContext().unbecome();
                        getSender().tell("initial", getSelf());
                    } else {
                        getSender().tell("switched", getSelf());
                    }
                })
                .build();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("switch")) {
                            getContext().become(switched);
                            getSender().tell("switched", getSelf());
                        } else {
                            getSender().tell("initial", getSelf());
                        }
                    })
                    .build();
        }
    }

    public static class SilentActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> {
            }).build();
        }
    }

    private record Watch(ActorRef actor) {
    }

    public static class WatcherActor extends AbstractActor {
        private String terminatedPath;
        private ActorRef waitingRequester;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Watch.class, request -> {
                        getContext().watch(request.actor());
                        getSender().tell("watching", getSelf());
                    })
                    .match(String.class, message -> {
                        if (message.equals("await-termination")) {
                            if (terminatedPath != null) getSender().tell(terminatedPath, getSelf());
                            else waitingRequester = getSender();
                        }
                    })
                    .match(Terminated.class, terminated -> {
                        terminatedPath = terminated.actor().path().toString();
                        if (waitingRequester != null) {
                            waitingRequester.tell(terminatedPath, getSelf());
                            waitingRequester = null;
                        }
                    })
                    .build();
        }
    }

    public static class RestartingActor extends AbstractActor {
        private final int instance;

        public RestartingActor(int instance) {
            this.instance = instance;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("fail")) throw new IllegalStateException("restart");
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class StrategyParent extends AbstractActor {
        private final SupervisorStrategy strategy;
        private final AtomicInteger firstInstances;
        private final AtomicInteger secondInstances;

        public StrategyParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            this.strategy = strategy;
            this.firstInstances = firstInstances;
            this.secondInstances = secondInstances;
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void preStart() {
            getContext().actorOf(Props.create(StrategyChild.class, firstInstances), "first");
            getContext().actorOf(Props.create(StrategyChild.class, secondInstances), "second");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("children")) {
                            getSender().tell(java.util.List.copyOf(getContext().getChildren()), getSelf());
                        } else if (message.equals("fail-first")) {
                            getContext().getChildren().stream()
                                    .filter(child -> child.path().name().equals("first"))
                                    .findFirst().orElseThrow().tell("fail");
                        }
                    })
                    .build();
        }
    }

    public static class AllForOneParent extends StrategyParent {
        public AllForOneParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            super(strategy, firstInstances, secondInstances);
        }
    }

    public static class OneForOneParent extends StrategyParent {
        public OneForOneParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            super(strategy, firstInstances, secondInstances);
        }
    }

    public static class StrategyChild extends AbstractActor {
        private final int instance;

        public StrategyChild(AtomicInteger instances) {
            instance = instances.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("fail")) throw new IllegalStateException("strategy failure");
                if (message.equals("instance")) getSender().tell(instance, getSelf());
            }).build();
        }
    }
}
