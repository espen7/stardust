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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.dispatch.sysmsg.Suspend;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MailboxTest {
    @Test
    void interruptedBoundedEnqueuePropagatesInterruptedException() throws Exception {
        try (ActorSystem system = ActorSystem.create("mailbox-interrupted")) {
            for (Duration pushTimeout : List.of(Duration.ofSeconds(1), Duration.ofSeconds(-1))) {
                BoundedMailbox.MessageQueue queue = new BoundedMailbox.MessageQueue(1, pushTimeout);
                queue.enqueue(system.deadLetters(), Envelope.apply("first", null, system));
                Thread.currentThread().interrupt();
                try {
                    assertThrows(InterruptedException.class,
                            () -> queue.enqueue(system.deadLetters(), Envelope.apply("second", null, system)));
                } finally {
                    Thread.interrupted();
                }
                assertEquals(1, queue.size());
            }
        }
    }

    @Test
    void systemMessagesDrainBeforeAnInterruptionReachesTheExecutor() throws Exception {
        try (ActorSystem system = ActorSystem.create("mailbox-system-interrupted")) {
            ExecutorServiceFactory factory = Executors::newSingleThreadExecutor;
            List<Object> processed = new ArrayList<>();
            Dispatcher dispatcher = new Dispatcher(DispatcherTestSupport.configurator(system, system.scheduler()),
                    Dispatchers.DefaultDispatcherId, 2, Duration.ZERO,
                    (id, threadFactory) -> factory, Duration.ofHours(1)) {
                @Override
                public boolean registerForExecution(Mailbox mailbox, boolean hasMessageHint, boolean hasSystemMessageHint) {
                    return false;
                }

                @Override
                public void suspend(ActorCell actor) {
                    processed.add("system");
                    if (processed.size() == 1) {
                        actor.mailbox().systemEnqueue(system.deadLetters(), new Suspend());
                        Thread.currentThread().interrupt();
                    }
                }
            };
            try {
                ActorCell cell = new ActorCell((ActorSystemImpl) system, null, null, dispatcher, null) {
                    @Override
                    public void invoke(Envelope message) {
                        processed.add(message.message());
                        if (message.message().equals("first")) Thread.currentThread().interrupt();
                    }
                };
                Mailbox mailbox = new DefaultSystemMessageQueue(new UnboundedMailbox.MessageQueue()) {};
                mailbox.setActor(cell);
                cell.swapMailbox(mailbox);
                mailbox.systemEnqueue(system.deadLetters(), new Suspend());
                assertTrue(mailbox.hasSystemMessages());
                assertFalse(mailbox.hasMessages());
                assertEquals(0, mailbox.numberOfMessages());
                mailbox.enqueue(system.deadLetters(), Envelope.apply("first", null, system));
                mailbox.enqueue(system.deadLetters(), Envelope.apply("second", null, system));

                assertEquals("Interrupted while processing system messages",
                        assertThrows(InterruptedException.class, mailbox::run).getMessage());
                assertEquals(List.of("system", "system"), processed);
                assertFalse(mailbox.hasSystemMessages());
                assertEquals(2, mailbox.numberOfMessages());

                assertEquals("Interrupted while processing actor messages",
                        assertThrows(InterruptedException.class, mailbox::run).getMessage());
                assertEquals(List.of("system", "system", "first"), processed);
                assertEquals(1, mailbox.numberOfMessages());
                mailbox.run();
                assertEquals(List.of("system", "system", "first", "second"), processed);
                assertFalse(mailbox.hasMessages());
            } finally {
                Thread.interrupted();
                dispatcher.shutdown();
            }
        }
    }

    @Test
    void throughputDeadlineEndsTheCurrentMailboxBatch() throws Exception {
        var config = ConfigFactory.parseString("""
                tapio.actor.default-dispatcher.throughput = 2
                tapio.actor.default-dispatcher.throughput-deadline-time = 1 ms
                """);
        try (ActorSystem system = ActorSystem.create("mailbox-deadline", config)) {
            Duration deadline = system.dispatcher().throughputDeadlineTime();
            assertEquals(Duration.ofMillis(1), deadline);
            ExecutorServiceFactory factory = () -> Executors.newSingleThreadExecutor();
            Dispatcher dispatcher = new Dispatcher(DispatcherTestSupport.configurator(system, system.scheduler()),
                    Dispatchers.DefaultDispatcherId, system.dispatcher().throughput(), deadline,
                    (id, threadFactory) -> factory, Duration.ofHours(1)) {
                @Override
                public boolean registerForExecution(Mailbox mailbox, boolean hasMessageHint, boolean hasSystemMessageHint) {
                    return false;
                }
            };
            try {
                AtomicInteger processed = new AtomicInteger();
                ActorCell cell = new ActorCell((ActorSystemImpl) system, null, null, dispatcher, null) {
                    @Override
                    public void invoke(Envelope message) {
                        if (processed.incrementAndGet() == 1) {
                            long end = System.nanoTime() + deadline.toNanos();
                            while (System.nanoTime() - end < 0) Thread.onSpinWait();
                        }
                    }
                };
                Mailbox mailbox = new DefaultSystemMessageQueue(new UnboundedMailbox.MessageQueue()) {};
                mailbox.setActor(cell);
                mailbox.enqueue(system.deadLetters(), Envelope.apply("first", null, system));
                mailbox.enqueue(system.deadLetters(), Envelope.apply("second", null, system));

                mailbox.run();
                assertEquals(1, processed.get());
                mailbox.run();
                assertEquals(2, processed.get());
            } finally {
                dispatcher.shutdown();
            }
        }
    }
}
