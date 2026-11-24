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
package io.github.espen7.stardust.dispatch;

import com.typesafe.config.ConfigFactory;
import io.github.espen7.stardust.actor.ActorCell;
import io.github.espen7.stardust.actor.ActorSystem;
import io.github.espen7.stardust.actor.ActorSystemImpl;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MailboxTest {
    @Test
    void throughputDeadlineEndsTheCurrentMailboxBatch() {
        var config = ConfigFactory.parseString("""
                pekko.actor.default-dispatcher.throughput = 2
                pekko.actor.default-dispatcher.throughput-deadline-time = 1 ms
                """);
        try (ActorSystem system = ActorSystem.create("mailbox-deadline", config)) {
            Duration deadline = system.dispatcher().throughputDeadlineTime();
            assertEquals(Duration.ofMillis(1), deadline);
            try (Dispatcher dispatcher = new Dispatcher(system.dispatcher().throughput(), deadline, system) {
                @Override
                public boolean registerForExecution(Mailbox mailbox, boolean hasMessageHint, boolean hasSystemMessageHint) {
                    return false;
                }
            }) {
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
                Mailbox mailbox = cell.mailbox();
                mailbox.enqueue(system.deadLetters(), Envelope.apply("first", null, system));
                mailbox.enqueue(system.deadLetters(), Envelope.apply("second", null, system));

                mailbox.run();
                assertEquals(1, processed.get());
                mailbox.run();
                assertEquals(2, processed.get());
            }
        }
    }
}
