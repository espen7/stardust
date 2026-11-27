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
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.event.LoggingFilter;
import io.github.espen7.tapio.japi.pf.ReceiveBuilder;
import io.github.espen7.tapio.pattern.AskTimeoutException;
import io.github.espen7.tapio.pattern.CircuitBreaker;
import io.github.espen7.tapio.pattern.Patterns;
import io.github.espen7.tapio.util.WildcardIndex;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 针对 Pekko 对齐修复与补全项的回归测试。 */
class PortingAlignmentFixTest {

    /** 自定义测试用 LoggingFilter，用于验证 LoggingAdapter 是否接入。 */
    public static final class BlockInfoFilter implements LoggingFilter {
        public BlockInfoFilter(ActorSystem.Settings settings, EventStream eventStream) {
        }

        @Override
        public boolean isErrorEnabled(Class<?> logClass, String logSource) {
            return true;
        }

        @Override
        public boolean isWarningEnabled(Class<?> logClass, String logSource) {
            return true;
        }

        @Override
        public boolean isInfoEnabled(Class<?> logClass, String logSource) {
            return false;
        }

        @Override
        public boolean isDebugEnabled(Class<?> logClass, String logSource) {
            return false;
        }
    }

    enum TestState {
        IDLE, ACTIVE
    }

    static final class FsmUnderTest extends AbstractFSM<TestState, String> {
        private final BlockingQueue<Object> observed;

        FsmUnderTest(BlockingQueue<Object> observed) {
            this.observed = observed;
            startWith(TestState.IDLE, "init");
            when(TestState.IDLE, Duration.ofMillis(80), matchEventEquals("cancel-timeout", (msg, data) ->
                            goTo(TestState.IDLE).forMax(null))
                    .matchEventEquals("start-timer", (msg, data) -> {
                        startSingleTimer("tick", "timer-fired", Duration.ofMillis(30));
                        observed.add("timer-active:" + isTimerActive("tick"));
                        return stay();
                    })
                    .matchEventEquals("timer-fired", (msg, data) -> {
                        observed.add("timer-fired:" + isTimerActive("tick"));
                        return goTo(TestState.ACTIVE);
                    })
                    .matchEventEquals(FSM.StateTimeout, (msg, data) -> {
                        observed.add("state-timeout");
                        return stay();
                    }));
            when(TestState.ACTIVE, matchEventEquals("back", (msg, data) -> goTo(TestState.IDLE)));
            initialize();
        }
    }

    static final class ListenerActor extends AbstractActor {
        private final BlockingQueue<Object> received;

        ListenerActor(BlockingQueue<Object> received) {
            this.received = received;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(received::add)
                    .build();
        }
    }

    static final class SilentActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .matchEquals("ping", s -> getSender().tell("pong", getSelf()))
                    .build();
        }
    }

    @Test
    void receiveBuilderSupportsMatchEqualsUncheckedAndBooleanSupplier() throws Exception {
        AtomicBoolean gate = new AtomicBoolean(false);
        AtomicInteger hits = new AtomicInteger();
        AbstractActor.Receive receive = ReceiveBuilder.create()
                .matchEquals("exact", s -> hits.addAndGet(1))
                .matchEquals("gated", gate::get, s -> hits.addAndGet(10))
                .matchUnchecked(List.class, (List<String> list) -> hits.addAndGet(list.size()))
                .match(Integer.class, gate::get, n -> hits.addAndGet(n))
                .build();

        assertTrue(receive.onMessage().isDefinedAt("exact"));
        receive.onMessage().apply("exact");
        assertFalse(receive.onMessage().isDefinedAt("gated"));
        gate.set(true);
        assertTrue(receive.onMessage().isDefinedAt("gated"));
        receive.onMessage().apply("gated");
        receive.onMessage().apply(List.of("a", "b"));
        receive.onMessage().apply(5);
        assertEquals(18, hits.get());
    }

    @Test
    void loggingAdapterRespectsConfiguredLoggingFilter() throws Exception {
        Config config = ConfigFactory.parseString(
                "tapio.logging-filter = \"" + BlockInfoFilter.class.getName() + "\"\n"
                        + "tapio.loglevel = \"DEBUG\"\n");
        ActorSystem system = ActorSystem.create("log-filter-test", config);
        try {
            BlockingQueue<LogEvent> events = new LinkedBlockingQueue<>();
            ActorRef listener = system.actorOf(Props.create(ListenerActor.class, (BlockingQueue<Object>) (BlockingQueue<?>) events));
            system.eventStream().subscribe(listener, LogEvent.class);

            assertFalse(system.log().isInfoEnabled());
            assertTrue(system.log().isErrorEnabled());
            system.log().info("filtered-out");
            system.log().error("allowed-error");

            LogEvent event = events.poll(3, TimeUnit.SECONDS);
            assertInstanceOf(Error.class, event);
            assertEquals("allowed-error", event.message());
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void coordinatedShutdownIsExtensionAndEscapesActorTerminationTaskName() throws Exception {
        ActorSystem system = ActorSystem.create("cs-ext-test");
        try {
            CoordinatedShutdown cs = CoordinatedShutdown.get(system);
            assertSame(cs, system.extension(CoordinatedShutdown.lookup()));
            ActorRef target = system.actorOf(Props.create(SilentActor.class), "target");
            cs.addActorTerminationTask(
                    CoordinatedShutdown.PhaseServiceStop,
                    "stop / with # special chars",
                    target,
                    Optional.of(PoisonPill.getInstance()));
            Done done = cs.runAll(CoordinatedShutdown.actorSystemTerminateReason())
                    .toCompletableFuture()
                    .get(5, TimeUnit.SECONDS);
            assertSame(Done.done(), done);
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void fsmSupportsListenersNamedTimersAndForMaxNullCancel() throws Exception {
        ActorSystem system = ActorSystem.create("fsm-ext-test");
        try {
            BlockingQueue<Object> fsmObserved = new LinkedBlockingQueue<>();
            BlockingQueue<Object> listenerEvents = new LinkedBlockingQueue<>();
            ActorRef fsm = system.actorOf(Props.create(FsmUnderTest.class, fsmObserved), "fsm");
            ActorRef listener = system.actorOf(Props.create(ListenerActor.class, listenerEvents), "listener");

            fsm.tell(new FSM.SubscribeTransitionCallBack(listener), ActorRef.noSender);
            Object first = listenerEvents.poll(3, TimeUnit.SECONDS);
            assertEquals(new FSM.CurrentState<>(fsm, TestState.IDLE), first);

            fsm.tell("cancel-timeout", ActorRef.noSender);
            assertEquals(null, fsmObserved.poll(150, TimeUnit.MILLISECONDS));

            fsm.tell("unhandled-msg", ActorRef.noSender);
            fsm.tell("start-timer", ActorRef.noSender);
            assertEquals("timer-active:true", fsmObserved.poll(3, TimeUnit.SECONDS));
            assertEquals("timer-fired:false", fsmObserved.poll(3, TimeUnit.SECONDS));

            Object selfTransition = listenerEvents.poll(3, TimeUnit.SECONDS);
            assertEquals(new FSM.Transition<>(fsm, TestState.IDLE, TestState.IDLE), selfTransition);

            Object transition = listenerEvents.poll(3, TimeUnit.SECONDS);
            assertEquals(new FSM.Transition<>(fsm, TestState.IDLE, TestState.ACTIVE), transition);

            fsm.tell(new FSM.UnsubscribeTransitionCallBack(listener), ActorRef.noSender);
            fsm.tell("back", ActorRef.noSender);
            assertEquals(null, listenerEvents.poll(150, TimeUnit.MILLISECONDS));
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void wildcardIndexHandlesPrefixIsEmptyAndSiblingBranches() {
        WildcardIndex<String> index = new WildcardIndex<String>()
                .insert(new String[]{"worker*", "a"}, "A")
                .insert(new String[]{"worker*", "b"}, "B");
        assertFalse(index.isEmpty());
        assertEquals(Optional.of("A"), index.find(List.of("worker-1", "a")));
        assertEquals(Optional.of("B"), index.find(List.of("worker-2", "b")));
    }

    @Test
    void gracefulStopFailsWithAskTimeoutExceptionOnTimeout() throws Exception {
        ActorSystem system = ActorSystem.create("graceful-timeout-test");
        try {
            ActorRef target = system.actorOf(Props.create(SilentActor.class), "silent");
            CompletionStage<Boolean> stage = Patterns.gracefulStop(target, Duration.ofMillis(50), "ping");
            ExecutionException thrown = assertThrows(ExecutionException.class,
                    () -> stage.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertInstanceOf(AskTimeoutException.class, thrown.getCause());
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void circuitBreakerPreservesListenersAndEnforcesSyncCallTimeout() throws Exception {
        ActorSystem system = ActorSystem.create("cb-test");
        try {
            CountDownLatch opened = new CountDownLatch(1);
            CircuitBreaker breaker = CircuitBreaker.create(
                            system.scheduler(), 1, Duration.ofMillis(30), Duration.ofSeconds(1))
                    .onOpen(opened::countDown)
                    .withExponentialBackoff(Duration.ofSeconds(5));

            assertThrows(TimeoutException.class, () -> breaker.callWithSyncCircuitBreaker(() -> {
                Thread.sleep(60);
                return "late";
            }));
            assertTrue(opened.await(3, TimeUnit.SECONDS));
            assertTrue(breaker.isOpen());
        } finally {
            system.terminate().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void singletonsPreserveIdentityAfterSerialization() throws Exception {
        assertSame(Done.getInstance(), roundTrip(Done.getInstance()));
        assertSame(Kill.getInstance(), roundTrip(Kill.getInstance()));
        assertSame(PoisonPill.getInstance(), roundTrip(PoisonPill.getInstance()));
    }

    private static Object roundTrip(Object value) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            return ois.readObject();
        }
    }
}
