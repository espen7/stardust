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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.event.Debug;
import io.github.espen7.tapio.event.LoggingReceive;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Batch3MailboxAndLoggingTest {
    record Urgent(String text) implements ControlMessage {}

    static final class TestPriorityMailbox extends UnboundedPriorityMailbox {
        TestPriorityMailbox() {
            super(PriorityGenerator.create(msg -> {
                if ("high".equals(msg)) return 0;
                if ("low".equals(msg)) return 2;
                return 1;
            }));
        }
    }

    static final class LoggingReceiveActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return LoggingReceive.create(getContext(), receiveBuilder()
                    .match(String.class, "ping"::equals, msg -> {})
                    .build());
        }
    }

    static final class DebugCaptureActor extends AbstractActor {
        private final List<String> messages;
        private final CountDownLatch latch;

        DebugCaptureActor(List<String> messages, CountDownLatch latch) {
            this.messages = messages;
            this.latch = latch;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Debug.class, debug -> {
                        messages.add(String.valueOf(debug.message()));
                        latch.countDown();
                    })
                    .build();
        }
    }

    @Test
    void priorityMailboxOrdersMessagesByGenerator() {
        try (ActorSystem system = ActorSystem.create("prio-mb")) {
            MessageQueue queue = new TestPriorityMailbox().create(ActorRef.noSender, system);
            queue.enqueue(ActorRef.noSender, new Envelope("low", ActorRef.noSender));
            queue.enqueue(ActorRef.noSender, new Envelope("normal", ActorRef.noSender));
            queue.enqueue(ActorRef.noSender, new Envelope("high", ActorRef.noSender));

            assertEquals("high", queue.dequeue().message());
            assertEquals("normal", queue.dequeue().message());
            assertEquals("low", queue.dequeue().message());
        }
    }

    @Test
    void controlAwareMailboxPrioritizesControlMessages() {
        try (ActorSystem system = ActorSystem.create("ctrl-mb")) {
            MessageQueue queue = new UnboundedControlAwareMailbox().create(ActorRef.noSender, system);
            queue.enqueue(ActorRef.noSender, new Envelope("normal-1", ActorRef.noSender));
            queue.enqueue(ActorRef.noSender, new Envelope("normal-2", ActorRef.noSender));
            queue.enqueue(ActorRef.noSender, new Envelope(new Urgent("stop-now"), ActorRef.noSender));

            assertEquals(new Urgent("stop-now"), queue.dequeue().message());
            assertEquals("normal-1", queue.dequeue().message());
            assertEquals("normal-2", queue.dequeue().message());
        }
    }

    @Test
    void loggingReceivePublishesDebugEventsWhenEnabled() throws Exception {
        Config cfg = ConfigFactory.parseMap(Map.of(
                "tapio.loglevel", "DEBUG",
                "tapio.actor.debug.receive", true));
        try (ActorSystem system = ActorSystem.create("logging-receive", cfg)) {
            List<String> messages = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(2);
            ActorRef probe = system.actorOf(Props.create(DebugCaptureActor.class, messages, latch), "probe");
            system.eventStream().subscribe(probe, Debug.class);

            ActorRef target = system.actorOf(Props.create(LoggingReceiveActor.class), "target");
            target.tell("ping", ActorRef.noSender);
            target.tell("other", ActorRef.noSender);

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertTrue(messages.stream().anyMatch(m -> m.contains("received handled message ping")));
            assertTrue(messages.stream().anyMatch(m -> m.contains("received unhandled message other")));
        }
    }

    @Test
    void registerOnTerminationRunsCallbackAfterClose() {
        AtomicBoolean ran = new AtomicBoolean(false);
        try (ActorSystem system = ActorSystem.create("term-cb")) {
            system.registerOnTermination(() -> ran.set(true));
        }
        assertTrue(ran.get());
    }
}
