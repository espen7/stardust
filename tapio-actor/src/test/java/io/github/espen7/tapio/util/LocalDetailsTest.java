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
package io.github.espen7.tapio.util;

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.setup.ActorSystemSetup;
import io.github.espen7.tapio.actor.setup.Setup;
import io.github.espen7.tapio.dispatch.DispatcherConfigurator;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Envelope;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.MessageQueue;
import io.github.espen7.tapio.dispatch.PriorityGenerator;
import io.github.espen7.tapio.dispatch.SingleConsumerOnlyUnboundedMailbox;
import io.github.espen7.tapio.dispatch.UnboundedPriorityMailbox;
import io.github.espen7.tapio.pattern.Patterns;
import io.github.espen7.tapio.pattern.RetrySupport;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalDetailsTest {
    static final class CustomSetup extends Setup {
        final String tag;

        CustomSetup(String tag) {
            this.tag = tag;
        }
    }

    static final class EchoActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, msg -> getSender().tell("echo:" + msg, getSelf()))
                    .build();
        }
    }

    @Test
    void stablePriorityQueuesPreserveFifoForEqualPriority() {
        record Item(int prio, String id) {}
        Comparator<Item> cmp = Comparator.comparingInt(Item::prio);
        StablePriorityQueue<Item> q1 = new StablePriorityQueue<>(4, cmp);
        StablePriorityBlockingQueue<Item> q2 = new StablePriorityBlockingQueue<>(4, cmp);

        for (int i = 0; i < 20; i++) {
            Item item = new Item(1, "id-" + i);
            q1.offer(item);
            q2.offer(item);
        }
        q1.offer(new Item(0, "urgent"));
        q2.offer(new Item(0, "urgent"));

        assertEquals("urgent", q1.poll().id());
        assertEquals("urgent", q2.poll().id());
        List<String> ids1 = new ArrayList<>();
        List<String> ids2 = new ArrayList<>();
        while (!q1.isEmpty()) ids1.add(q1.poll().id());
        while (!q2.isEmpty()) ids2.add(q2.poll().id());
        assertEquals(ids1, ids2);
        assertEquals("id-0", ids1.getFirst());
        assertEquals("id-19", ids1.getLast());

        try (ActorSystem system = ActorSystem.create("stable-prio-mb")) {
            MessageQueue mq = new UnboundedPriorityMailbox(
                    PriorityGenerator.create(m -> 1)).create(ActorRef.noSender, system);
            mq.enqueue(ActorRef.noSender, new Envelope("a", ActorRef.noSender));
            mq.enqueue(ActorRef.noSender, new Envelope("b", ActorRef.noSender));
            mq.enqueue(ActorRef.noSender, new Envelope("c", ActorRef.noSender));
            assertEquals("a", mq.dequeue().message());
            assertEquals("b", mq.dequeue().message());
            assertEquals("c", mq.dequeue().message());
        }
    }

    @Test
    void singleConsumerOnlyUnboundedMailboxDeliversMessagesInOrder() throws Exception {
        try (ActorSystem system = ActorSystem.create("sc-mb", ConfigFactory.parseMap(Map.of(
                "tapio.actor.sc-mailbox.mailbox-type", SingleConsumerOnlyUnboundedMailbox.class.getName())))) {
            ActorRef actor = system.actorOf(
                    Props.create(EchoActor.class).withMailbox("tapio.actor.sc-mailbox"), "echo");
            assertEquals("echo:hello",
                    Patterns.ask(actor, "hello", Duration.ofSeconds(1)).toCompletableFuture().get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void retrySupportRetriesWithBackoffUntilSuccess() throws Exception {
        try (ActorSystem system = ActorSystem.create("retry-test")) {
            AtomicInteger attempts = new AtomicInteger();
            String result = RetrySupport.retry(
                    () -> {
                        if (attempts.incrementAndGet() < 3) {
                            return CompletableFuture.failedFuture(new IllegalStateException("transient"));
                        }
                        return CompletableFuture.completedFuture("ok");
                    },
                    3,
                    Duration.ofMillis(10),
                    Duration.ofMillis(30),
                    0.0,
                    system.dispatcher(),
                    system.scheduler()).toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals("ok", result);
            assertEquals(3, attempts.get());
        }
    }

    @Test
    void dispatchersRegisterConfiguratorRegistersProgrammatically() {
        try (ActorSystem system = ActorSystem.create("reg-disp")) {
            DispatcherConfigurator custom = new DispatcherConfigurator(
                    system.dispatchers().config(Dispatchers.DefaultDispatcherId),
                    system.dispatchers().prerequisites());
            assertTrue(system.dispatchers().registerConfigurator("custom.disp", custom));
            assertFalse(system.dispatchers().registerConfigurator("custom.disp", custom));
            MessageDispatcher resolved = system.dispatchers().lookup("custom.disp");
            assertSame(custom.dispatcher(), resolved);
        }
    }

    @Test
    void actorSystemSetupIsAccessibleFromSettings() {
        ActorSystemSetup setup = new CustomSetup("v1").and(new CustomSetup("v2"));
        try (ActorSystem system = ActorSystem.create("setup-test", setup)) {
            assertEquals("v2", system.settings().setup.get(CustomSetup.class).orElseThrow().tag);
        }
    }
}
