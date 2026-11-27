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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Props;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class EventStreamTest {
    private static final Duration WAIT = Duration.ofSeconds(3);

    @Test
    void terminatedSubscriberIsAutomaticallyUnsubscribed() throws Exception {
        try (ActorSystem system = ActorSystem.create("event-stream-unsubscribe", ConfigFactory.parseString("""
                tapio.loggers = []
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """))) {
            CountDownLatch subscribed = new CountDownLatch(1);
            CountDownLatch stopped = new CountDownLatch(1);
            ActorRef subscriber = system.actorOf(Props.create(Subscriber.class, subscribed, stopped), "subscriber");

            assertTrue(subscribed.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(system.eventStream().hasSubscriptions(subscriber));
            system.stop(subscriber);
            assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));

            await("terminated subscriber was not unsubscribed",
                    () -> !system.eventStream().hasSubscriptions(subscriber));
            system.eventStream().publish(new ProbeEvent());
            assertFalse(system.eventStream().hasSubscriptions(subscriber));
        }
    }

    @Test
    void removingOneChannelKeepsTerminationWatchForRemainingSubscriptions() throws Exception {
        try (ActorSystem system = ActorSystem.create("event-stream-multi-channel", ConfigFactory.parseString("""
                tapio.loggers = []
                tapio.stdout-loglevel = OFF
                tapio.log-dead-letters = off
                """))) {
            CountDownLatch subscribed = new CountDownLatch(1);
            CountDownLatch stopped = new CountDownLatch(1);
            ActorRef subscriber = system.actorOf(Props.create(Subscriber.class, subscribed, stopped, true),
                    "subscriber");

            assertTrue(subscribed.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));
            assertTrue(system.eventStream().unsubscribe(subscriber, ProbeEvent.class));
            assertTrue(system.eventStream().hasSubscriptions(subscriber));
            system.stop(subscriber);
            assertTrue(stopped.await(WAIT.toMillis(), TimeUnit.MILLISECONDS));

            await("remaining subscription was not removed after termination",
                    () -> !system.eventStream().hasSubscriptions(subscriber));
        }
    }

    @Test
    void unsubscribeFromAnInheritedChannelKeepsItsParentAndSibling() {
        List<Object> received = new ArrayList<>();
        ActorRef subscriber = new ActorRef() {
            @Override
            public ActorPath path() {
                return new io.github.espen7.tapio.actor.RootActorPath(
                        new io.github.espen7.tapio.actor.Address(ActorPath.Scheme, "event-stream")).child("subscriber");
            }

            @Override
            public void tell(Object message, ActorRef sender) {
                received.add(message);
            }
        };
        EventStream stream = new EventStream(null);
        assertTrue(stream.subscribe(subscriber, Number.class));
        stream.publish(1);
        stream.publish(2L);
        assertTrue(stream.unsubscribe(subscriber, Integer.class));
        stream.publish(3);
        stream.publish(4L);
        assertEquals(List.of(1, 2L, 4L), received);
        assertTrue(stream.hasSubscriptions(subscriber));

        assertTrue(stream.unsubscribe(subscriber, Number.class));
        stream.publish(5L);
        assertFalse(stream.hasSubscriptions(subscriber));
        assertEquals(List.of(1, 2L, 4L), received);
    }

    private static void await(String message, BooleanSupplier condition) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail(message);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    public static final class Subscriber extends AbstractActor {
        private final CountDownLatch subscribed;
        private final CountDownLatch stopped;
        private final boolean multipleChannels;

        public Subscriber(CountDownLatch subscribed, CountDownLatch stopped) {
            this(subscribed, stopped, false);
        }

        public Subscriber(CountDownLatch subscribed, CountDownLatch stopped, boolean multipleChannels) {
            this.subscribed = subscribed;
            this.stopped = stopped;
            this.multipleChannels = multipleChannels;
        }

        @Override
        public void preStart() {
            getContext().system().eventStream().subscribe(getSelf(), ProbeEvent.class);
            if (multipleChannels) getContext().system().eventStream().subscribe(getSelf(), OtherProbeEvent.class);
            subscribed.countDown();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }

        @Override
        public void postStop() {
            stopped.countDown();
        }
    }

    private record ProbeEvent() {
    }

    private record OtherProbeEvent() {
    }
}
