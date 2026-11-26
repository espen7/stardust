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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.AbstractFSM;
import io.github.espen7.tapio.actor.AbstractLoggingActorWithStash;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.FSM;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.event.Info;
import io.github.espen7.tapio.event.LogEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Batch4And5Test {
    static final class FlakyChildActor extends AbstractActor {
        private final AtomicInteger starts;

        FlakyChildActor(AtomicInteger starts) {
            this.starts = starts;
        }

        @Override
        public void preStart() {
            starts.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "fail"::equals, msg -> {
                        throw new IllegalStateException("boom");
                    })
                    .match(String.class, "stop-self"::equals, msg -> getContext().stop(getSelf()))
                    .match(String.class, msg -> getSender().tell("ok:" + msg, getSelf()))
                    .build();
        }
    }

    enum FsmState {
        IDLE, ACTIVE
    }

    record FsmData(int count) {
    }

    static final class SampleFsmActor extends AbstractFSM<FsmState, FsmData> {
        private final List<String> transitions;
        private final CompletableFuture<FSM.Reason> stopReason;

        SampleFsmActor(List<String> transitions, CompletableFuture<FSM.Reason> stopReason) {
            this.transitions = transitions;
            this.stopReason = stopReason;

            startWith(FsmState.IDLE, new FsmData(0));

            when(FsmState.IDLE, stateFunction()
                    .matchEvent(String.class, "go"::equals, (ev, data) ->
                            goTo(FsmState.ACTIVE).using(new FsmData(data.count() + 1)).replying("entered"))
                    .matchEventEquals("get", (ev, data) -> stay().replying(data.count())));

            when(FsmState.ACTIVE, Duration.ofMillis(40), stateFunction()
                    .matchEventEquals(FSM.StateTimeout, (ev, data) -> goTo(FsmState.IDLE))
                    .matchEventEquals("stop", (ev, data) -> stop(FSM.normal(), data)));

            onTransition(transitionHandler()
                    .state(FsmState.IDLE, FsmState.ACTIVE, (f, t) -> transitions.add(f + "->" + t + ":" + nextStateData().count()))
                    .state(FsmState.ACTIVE, FsmState.IDLE, (f, t) -> transitions.add(f + "->" + t)));

            onTermination(ev -> stopReason.complete(ev.reason()));
            initialize();
        }
    }

    static final class FilteredLoggingStashActor extends AbstractLoggingActorWithStash {
        private final List<Object> handled = new CopyOnWriteArrayList<>();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "flush-nums"::equals, msg -> {
                        unstashAll(item -> item instanceof Integer);
                        getContext().become(receiveBuilder()
                                .match(String.class, "dump"::equals, m -> getSender().tell(List.copyOf(handled), getSelf()))
                                .matchAny(handled::add)
                                .build());
                    })
                    .matchAny(msg -> stash())
                    .build();
        }
    }

    static final class InfoCaptureActor extends AbstractActor {
        private final List<String> messages;
        private final CountDownLatch latch;

        InfoCaptureActor(List<String> messages, CountDownLatch latch) {
            this.messages = messages;
            this.latch = latch;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Info.class, info -> {
                        messages.add(String.valueOf(info.message()));
                        latch.countDown();
                    })
                    .build();
        }
    }

    @Test
    void backoffSupervisorRestartsOnFailureAndStopsOnFinalStopMessage() throws Exception {
        try (ActorSystem system = ActorSystem.create("backoff-test")) {
            AtomicInteger starts = new AtomicInteger();
            Props supProps = BackoffSupervisor.props(
                    BackoffOpts.onFailure(
                                    Props.create(FlakyChildActor.class, starts),
                                    "child",
                                    Duration.ofMillis(20),
                                    Duration.ofMillis(100),
                                    0.0)
                            .withManualReset()
                            .withReplyWhileStopped("busy"));

            ActorRef sup = system.actorOf(supProps, "sup-failure");
            assertEquals("ok:ping",
                    Patterns.ask(sup, "ping", Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS));

            sup.tell("fail", ActorRef.noSender);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (starts.get() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(15);
            }
            assertTrue(starts.get() >= 2);
            BackoffSupervisor.RestartCount rc = (BackoffSupervisor.RestartCount) Patterns.ask(
                    sup, BackoffSupervisor.getRestartCount(), Duration.ofSeconds(1))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(rc.count() >= 1);

            sup.tell(BackoffSupervisor.reset(), ActorRef.noSender);
            BackoffSupervisor.RestartCount afterReset = (BackoffSupervisor.RestartCount) Patterns.ask(
                    sup, BackoffSupervisor.getRestartCount(), Duration.ofSeconds(1))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(0, afterReset.count());

            AtomicInteger stopStarts = new AtomicInteger();
            ActorRef stopSup = system.actorOf(BackoffSupervisor.props(
                    BackoffOpts.onStop(
                                    Props.create(FlakyChildActor.class, stopStarts),
                                    "stop-child",
                                    Duration.ofMillis(20),
                                    Duration.ofMillis(100),
                                    0.0)
                            .withFinalStopMessage("stop-self"::equals)), "sup-stop");
            stopSup.tell("stop-self", ActorRef.noSender);
            assertEquals(Boolean.TRUE,
                    Patterns.gracefulStop(stopSup, Duration.ofSeconds(2), "stop-self")
                            .toCompletableFuture().get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void circuitBreakerTransitionsBetweenClosedOpenHalfOpenAndClosed() throws Exception {
        try (ActorSystem system = ActorSystem.create("cb-test")) {
            CountDownLatch openLatch = new CountDownLatch(1);
            CountDownLatch halfOpenLatch = new CountDownLatch(1);
            CountDownLatch closeLatch = new CountDownLatch(1);

            CircuitBreaker cb = CircuitBreaker.create(
                            system.scheduler(), 2, Duration.ofSeconds(1), Duration.ofMillis(40))
                    .onOpen(openLatch::countDown)
                    .onHalfOpen(halfOpenLatch::countDown)
                    .onClose(closeLatch::countDown);

            assertTrue(cb.isClosed());
            assertThrows(IllegalStateException.class, () -> cb.callWithSyncCircuitBreaker(() -> {
                throw new IllegalStateException("fail-1");
            }));
            assertEquals(1, cb.currentFailureCount());
            assertThrows(IllegalStateException.class, () -> cb.callWithSyncCircuitBreaker(() -> {
                throw new IllegalStateException("fail-2");
            }));

            assertTrue(openLatch.await(1, TimeUnit.SECONDS));
            assertTrue(cb.isOpen());
            ExecutionException fastFail = assertThrows(ExecutionException.class, () ->
                    cb.callWithCircuitBreaker(() -> CompletableFuture.completedFuture("nope"))
                            .toCompletableFuture().get(1, TimeUnit.SECONDS));
            assertInstanceOf(CircuitBreakerOpenException.class, fastFail.getCause());

            assertTrue(halfOpenLatch.await(2, TimeUnit.SECONDS));
            assertTrue(cb.isHalfOpen());
            assertEquals("recovered",
                    cb.callWithCircuitBreaker(() -> CompletableFuture.completedFuture("recovered"))
                            .toCompletableFuture().get(1, TimeUnit.SECONDS));
            assertTrue(closeLatch.await(1, TimeUnit.SECONDS));
            assertTrue(cb.isClosed());
        }
    }

    @Test
    void abstractFsmHandlesTransitionsTimeoutsAndTermination() throws Exception {
        try (ActorSystem system = ActorSystem.create("fsm-test")) {
            List<String> transitions = new CopyOnWriteArrayList<>();
            CompletableFuture<FSM.Reason> stopReason = new CompletableFuture<>();
            ActorRef fsm = system.actorOf(Props.create(SampleFsmActor.class, transitions, stopReason), "fsm");

            assertEquals("entered",
                    Patterns.ask(fsm, "go", Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (transitions.size() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(15);
            }
            assertEquals(List.of("IDLE->ACTIVE:1", "ACTIVE->IDLE"), transitions);

            fsm.tell("go", ActorRef.noSender);
            fsm.tell("stop", ActorRef.noSender);
            assertSameOrEqualNormal(stopReason.get(2, TimeUnit.SECONDS));
        }
    }

    private static void assertSameOrEqualNormal(FSM.Reason reason) {
        assertEquals(FSM.normal(), reason);
    }

    @Test
    void stashSupportsFilteredUnstashAndLoggingFilterTracksLevel() throws Exception {
        Config cfg = ConfigFactory.parseMap(Map.of("tapio.log-config-on-start", true));
        try (ActorSystem system = ActorSystem.create("batch5-test", cfg)) {
            ExtendedActorSystem ext = (ExtendedActorSystem) system;
            assertTrue(ext.logFilter().isInfoEnabled(getClass(), "test"));
            assertFalse(ext.logFilter().isDebugEnabled(getClass(), "test"));
            system.eventStream().setLogLevel(LogEvent.Level.DEBUG);
            assertTrue(ext.logFilter().isDebugEnabled(getClass(), "test"));

            List<String> infoLogs = new CopyOnWriteArrayList<>();
            CountDownLatch infoLatch = new CountDownLatch(1);
            ActorRef probe = system.actorOf(Props.create(InfoCaptureActor.class, infoLogs, infoLatch), "info-probe");
            system.eventStream().subscribe(probe, Info.class);
            system.logConfiguration();
            assertTrue(infoLatch.await(2, TimeUnit.SECONDS));
            assertFalse(infoLogs.isEmpty());

            ActorRef stashActor = system.actorOf(Props.create(FilteredLoggingStashActor.class), "stash");
            stashActor.tell("skip-me", ActorRef.noSender);
            stashActor.tell(1, ActorRef.noSender);
            stashActor.tell(2, ActorRef.noSender);
            stashActor.tell("flush-nums", ActorRef.noSender);
            List<?> dumped = (List<?>) Patterns.ask(stashActor, "dump", Duration.ofSeconds(1))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of(1, 2), dumped);
        }
    }
}
