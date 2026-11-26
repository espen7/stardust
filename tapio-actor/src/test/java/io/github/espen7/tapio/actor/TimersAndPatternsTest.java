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

import io.github.espen7.tapio.pattern.AskTimeoutException;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimersAndPatternsTest {
    private record StartSingle(Object key, Object msg, Duration delay) {}
    private record StartFixedDelay(Object key, Object msg, Duration delay) {}
    private record CancelKey(Object key) {}
    private record CheckActive(Object key) {}

    static final class TimerProbeActor extends AbstractLoggingActorWithTimers {
        private final BlockingQueue<Object> received;
        private final AtomicInteger restarts;

        TimerProbeActor(BlockingQueue<Object> received, AtomicInteger restarts) {
            this.received = received;
            this.restarts = restarts;
        }

        @Override
        public void preRestart(Throwable reason, java.util.Optional<Object> message) throws Exception {
            restarts.incrementAndGet();
            super.preRestart(reason, message);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(StartSingle.class, s -> getTimers().startSingleTimer(s.key(), s.msg(), s.delay()))
                    .match(StartFixedDelay.class, s -> getTimers().startTimerWithFixedDelay(s.key(), s.msg(), s.delay()))
                    .match(CancelKey.class, c -> getTimers().cancel(c.key()))
                    .match(CheckActive.class, c -> getSender().tell(getTimers().isTimerActive(c.key()), getSelf()))
                    .match(String.class, "fail"::equals, msg -> {
                        throw new IllegalStateException("boom");
                    })
                    .matchAny(received::add)
                    .build();
        }
    }

    static final class PipeReceiverActor extends AbstractActor {
        private final List<Object> messages = new CopyOnWriteArrayList<>();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "dump"::equals, msg -> getSender().tell(List.copyOf(messages), getSelf()))
                    .matchAny(messages::add)
                    .build();
        }
    }

    static final class CustomStopActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "custom-stop"::equals, msg -> getContext().stop(getSelf()))
                    .match(String.class, "ignore"::equals, msg -> {})
                    .build();
        }
    }

    @Test
    void timersSupportSinglePeriodicOverwriteCancelAndRestartCleanup() throws Exception {
        try (ActorSystem system = ActorSystem.create("timers-test")) {
            BlockingQueue<Object> received = new LinkedBlockingQueue<>();
            AtomicInteger restarts = new AtomicInteger();
            ActorRef actor = system.actorOf(Props.create(TimerProbeActor.class, received, restarts), "timer");

            actor.tell(new StartSingle("k1", "v1", Duration.ofMillis(30)), ActorRef.noSender);
            assertEquals(Boolean.TRUE,
                    Patterns.ask(actor, new CheckActive("k1"), Duration.ofSeconds(1)).toCompletableFuture().get());
            assertEquals("v1", received.poll(2, TimeUnit.SECONDS));
            assertEquals(Boolean.FALSE,
                    Patterns.ask(actor, new CheckActive("k1"), Duration.ofSeconds(1)).toCompletableFuture().get());

            actor.tell(new StartFixedDelay("k2", "tick", Duration.ofMillis(20)), ActorRef.noSender);
            assertEquals("tick", received.poll(2, TimeUnit.SECONDS));
            assertEquals("tick", received.poll(2, TimeUnit.SECONDS));
            actor.tell(new CancelKey("k2"), ActorRef.noSender);
            assertEquals(Boolean.FALSE,
                    Patterns.ask(actor, new CheckActive("k2"), Duration.ofSeconds(1)).toCompletableFuture().get());

            actor.tell(new StartFixedDelay("k3", "stale", Duration.ofMillis(20)), ActorRef.noSender);
            actor.tell("fail", ActorRef.noSender);
            assertEquals(Boolean.FALSE,
                    Patterns.ask(actor, new CheckActive("k3"), Duration.ofSeconds(1)).toCompletableFuture().get());
            assertTrue(restarts.get() >= 1);
            assertFalse(received.contains("stale"));
        }
    }

    @Test
    void pipeDeliversSuccessAndFailureToActorAndSelection() throws Exception {
        try (ActorSystem system = ActorSystem.create("pipe-test")) {
            ActorRef receiver = system.actorOf(Props.create(PipeReceiverActor.class), "receiver");
            CompletableFuture<String> success = CompletableFuture.completedFuture("ok");
            CompletableFuture<String> failure = CompletableFuture.failedFuture(new IllegalArgumentException("bad"));

            Patterns.pipe(success, system.dispatcher()).to(receiver);
            Patterns.pipe(failure, null).to(system.actorSelection("/user/receiver"));

            List<?> dumped = List.of();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (dumped.size() < 2 && System.nanoTime() < deadline) {
                dumped = (List<?>) Patterns.ask(receiver, "dump", Duration.ofSeconds(1)).toCompletableFuture().get();
                if (dumped.size() < 2) {
                    Thread.sleep(10);
                }
            }
            assertEquals(2, dumped.size());
            assertTrue(dumped.contains("ok"));
            assertTrue(dumped.stream().anyMatch(item -> item instanceof Status.Failure));
        }
    }

    @Test
    void gracefulStopCompletesOnStopAndTimesOutOtherwise() throws Exception {
        try (ActorSystem system = ActorSystem.create("graceful-stop-test")) {
            ActorRef a1 = system.actorOf(Props.create(CustomStopActor.class), "a1");
            assertEquals(Boolean.TRUE,
                    Patterns.gracefulStop(a1, Duration.ofSeconds(2)).toCompletableFuture().get(2, TimeUnit.SECONDS));

            ActorRef a2 = system.actorOf(Props.create(CustomStopActor.class), "a2");
            assertEquals(Boolean.TRUE,
                    Patterns.gracefulStop(a2, Duration.ofSeconds(2), "custom-stop")
                            .toCompletableFuture().get(2, TimeUnit.SECONDS));

            ActorRef a3 = system.actorOf(Props.create(CustomStopActor.class), "a3");
            ExecutionException ex = assertThrows(ExecutionException.class, () ->
                    Patterns.gracefulStop(a3, Duration.ofMillis(40), "ignore")
                            .toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertInstanceOf(AskTimeoutException.class, ex.getCause());
        }
    }
}
