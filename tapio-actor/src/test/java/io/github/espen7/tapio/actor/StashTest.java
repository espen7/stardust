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
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StashTest {
    static final class ProtocolActor extends AbstractActorWithStash {
        private final List<String> processed = new CopyOnWriteArrayList<>();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "open"::equals, msg -> {
                        unstashAll();
                        getContext().become(open(), false);
                    })
                    .match(String.class, "dump"::equals, msg -> getSender().tell(List.copyOf(processed), getSelf()))
                    .match(String.class, msg -> stash())
                    .build();
        }

        private Receive open() {
            return receiveBuilder()
                    .match(String.class, "close"::equals, msg -> getContext().unbecome())
                    .match(String.class, "dump"::equals, msg -> getSender().tell(List.copyOf(processed), getSelf()))
                    .match(String.class, processed::add)
                    .build();
        }
    }

    static final class CapacityActor extends AbstractActorWithUnboundedStash {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, msg -> {
                        try {
                            stash();
                            getSender().tell("stashed:" + msg, getSelf());
                        } catch (StashOverflowException overflow) {
                            getSender().tell("overflow:" + msg, getSelf());
                        }
                    })
                    .build();
        }
    }

    static final class DeadLetterProbe extends AbstractActor {
        private final List<DeadLetter> deadLetters;
        private final CountDownLatch latch;

        DeadLetterProbe(List<DeadLetter> deadLetters, CountDownLatch latch) {
            this.deadLetters = deadLetters;
            this.latch = latch;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(DeadLetter.class, dl -> {
                        if ("lost".equals(dl.message())) {
                            deadLetters.add(dl);
                            latch.countDown();
                        }
                    })
                    .build();
        }
    }

    @Test
    void stashAndUnstashAllPreserveMessageOrderAcrossBecome() throws Exception {
        try (ActorSystem system = ActorSystem.create("stash-order")) {
            ActorRef actor = system.actorOf(Props.create(ProtocolActor.class), "protocol");
            actor.tell("m1", ActorRef.noSender);
            actor.tell("m2", ActorRef.noSender);
            actor.tell("open", ActorRef.noSender);
            actor.tell("m3", ActorRef.noSender);

            List<?> processed = (List<?>) Patterns.ask(actor, "dump", Duration.ofSeconds(1))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(List.of("m1", "m2", "m3"), processed);
        }
    }

    @Test
    void stashCapacityOverflowThrowsStashOverflowException() throws Exception {
        Config cfg = ConfigFactory.parseMap(Map.of("tapio.actor.default-mailbox.stash-capacity", 1));
        try (ActorSystem system = ActorSystem.create("stash-capacity", cfg)) {
            ActorRef actor = system.actorOf(Props.create(CapacityActor.class), "cap");
            assertEquals("stashed:first",
                    Patterns.ask(actor, "first", Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS));
            assertEquals("overflow:second",
                    Patterns.ask(actor, "second", Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void postStopUnstashesRemainingMessagesToDeadLetters() throws Exception {
        try (ActorSystem system = ActorSystem.create("stash-stop")) {
            List<DeadLetter> deadLetters = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            ActorRef probe = system.actorOf(Props.create(DeadLetterProbe.class, deadLetters, latch), "dl-probe");
            system.eventStream().subscribe(probe, DeadLetter.class);

            ActorRef actor = system.actorOf(Props.create(ProtocolActor.class), "protocol");
            actor.tell("lost", ActorRef.noSender);
            Patterns.gracefulStop(actor, Duration.ofSeconds(2)).toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertEquals(1, deadLetters.size());
        }
    }
}
