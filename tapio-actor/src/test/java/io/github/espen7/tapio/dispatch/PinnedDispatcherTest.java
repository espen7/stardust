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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorRefWithCell;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.InternalActorRef;
import io.github.espen7.tapio.actor.LocalActorRef;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.actor.Terminated;
import io.github.espen7.tapio.event.Warning;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PinnedDispatcherTest {
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;
    private final List<LocalActorRef> detachedActors = new CopyOnWriteArrayList<>();

    @AfterEach
    void stopSystem() throws Exception {
        for (LocalActorRef actor : detachedActors) {
            if (!actor.isTerminated()) actor.stop();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        for (LocalActorRef actor : detachedActors) {
            while (!actor.isTerminated() && System.nanoTime() < deadline) Thread.yield();
            assertTrue(actor.isTerminated(), "detached actor did not stop: " + actor);
        }
        if (system != null) system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void eachLookupCreatesAnIndependentSingleThreadDispatcher() throws Exception {
        system = ActorSystem.create("pinned-independent", pinnedConfig());
        ActorRef first = system.actorOf(Props.create(ThreadNameActor.class)
                .withDispatcher("dispatchers.pinned"), "first");
        ActorRef second = system.actorOf(Props.create(ThreadNameActor.class)
                .withDispatcher("dispatchers.pinned-alias"), "second");

        String firstThread = (String) ask(first, "one");
        String firstThreadAgain = (String) ask(first, "two");
        String secondThread = (String) ask(second, "one");
        MessageDispatcher firstDispatcher = underlying(first).dispatcher();
        MessageDispatcher secondDispatcher = underlying(second).dispatcher();
        assertInstanceOf(PinnedDispatcher.class, firstDispatcher);
        assertInstanceOf(PinnedDispatcher.class, secondDispatcher);
        assertNotSame(firstDispatcher, secondDispatcher);
        assertEquals(Integer.MAX_VALUE, firstDispatcher.throughput());
        assertEquals(firstThread, firstThreadAgain);
        assertNotEquals(firstThread, secondThread);
        assertTrue(firstThread.startsWith("pinned-independent-dispatchers.pinned-"));
        assertTrue(secondThread.startsWith("pinned-independent-dispatchers.pinned-"));
    }

    @Test
    void dispatcherRejectsSecondOwnerAndAcceptsNewOwnerAfterDetach() throws Exception {
        system = ActorSystem.create("pinned-owner", pinnedConfig());
        PinnedDispatcher dispatcher = (PinnedDispatcher) system.dispatchers().lookup("dispatchers.pinned");
        CountDownLatch firstReady = new CountDownLatch(1);
        CountDownLatch firstStopped = new CountDownLatch(1);
        LocalActorRef first = detachedActor(dispatcher, "first", firstReady, firstStopped);
        assertTrue(firstReady.await(WAIT_SECONDS, TimeUnit.SECONDS));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> detachedActor(dispatcher, "second", new CountDownLatch(1), new CountDownLatch(1)));
        assertTrue(error.getMessage().startsWith("Cannot register to anyone but "));

        BlockingQueue<ActorRef> terminated = new LinkedBlockingQueue<>();
        ActorRef watcher = system.actorOf(Props.create(WatchProbe.class, terminated), "watcher");
        assertEquals("watching", ask(watcher, new WatchCommand(first)));
        first.stop();
        assertSame(first, terminated.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertTrue(firstStopped.await(WAIT_SECONDS, TimeUnit.SECONDS));

        CountDownLatch thirdReady = new CountDownLatch(1);
        CountDownLatch thirdStopped = new CountDownLatch(1);
        LocalActorRef third = detachedActor(dispatcher, "third", thirdReady, thirdStopped);
        assertTrue(thirdReady.await(WAIT_SECONDS, TimeUnit.SECONDS));
        third.stop();
        assertTrue(thirdStopped.await(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void nonThreadPoolExecutorWarnsAndFallsBackToDefaultPinnedConfig() throws Exception {
        system = ActorSystem.create("pinned-fallback", ConfigFactory.parseString("""
                dispatchers.pinned {
                  type = "PinnedDispatcher"
                  executor = "fork-join-executor"
                  shutdown-timeout = 1h
                }
                """));
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef collector = system.actorOf(Props.create(EventCollector.class, events), "events");
        system.eventStream().subscribe(collector, Warning.class);

        MessageDispatcher dispatcher = system.dispatchers().lookup("dispatchers.pinned");
        assertInstanceOf(PinnedDispatcher.class, dispatcher);
        Warning warning = assertInstanceOf(Warning.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("PinnedDispatcherConfigurator", warning.source());
        assertEquals("PinnedDispatcher [dispatchers.pinned] not configured to use ThreadPoolExecutor, "
                + "falling back to default config.", warning.message());

        ActorRef actor = system.actorOf(Props.create(ThreadNameActor.class)
                .withDispatcher("dispatchers.pinned"), "actor");
        assertTrue(((String) ask(actor, "thread")).startsWith("pinned-fallback-dispatchers.pinned-"));
    }

    @Test
    void unsupportedExecutorFailsDuringPinnedLookup() {
        system = ActorSystem.create("pinned-invalid", ConfigFactory.parseString("""
                dispatchers.pinned {
                  type = "PinnedDispatcher"
                  executor = "unsupported-executor"
                }
                """));

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> system.dispatchers().lookup("dispatchers.pinned"));
        assertTrue(error.getMessage().contains("Supported built-ins"));
    }

    private LocalActorRef detachedActor(MessageDispatcher dispatcher, String name, CountDownLatch ready,
                                        CountDownLatch stopped) {
        LocalActorRef actor = new LocalActorRef((ActorSystemImpl) system,
                Props.create(LifecycleActor.class, ready, stopped), dispatcher, new UnboundedMailbox(),
                (InternalActorRef) system.deadLetters(),
                new io.github.espen7.tapio.actor.RootActorPath(
                        new io.github.espen7.tapio.actor.Address(ActorPath.Scheme, ((ActorSystemImpl) system).name()))
                        .child("user").child(name),
                name);
        detachedActors.add(actor);
        return actor;
    }

    private static ActorCell underlying(ActorRef actor) {
        return (ActorCell) ((ActorRefWithCell) actor).underlying();
    }

    private static Object ask(ActorRef actor, Object message) throws Exception {
        return Patterns.ask(actor, message, Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static Config pinnedConfig() {
        return ConfigFactory.parseString("""
                dispatchers.pinned {
                  type = "PinnedDispatcher"
                  executor = "thread-pool-executor"
                  shutdown-timeout = 1h
                  thread-pool-executor {
                    fixed-pool-size = 8
                    allow-core-timeout = off
                  }
                }
                dispatchers.pinned-alias = "dispatchers.pinned"
                """);
    }

    public static final class ThreadNameActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class,
                    message -> getSender().tell(Thread.currentThread().getName(), getSelf())).build();
        }
    }

    public static final class LifecycleActor extends AbstractActor {
        private final CountDownLatch ready;
        private final CountDownLatch stopped;

        public LifecycleActor(CountDownLatch ready, CountDownLatch stopped) {
            this.ready = ready;
            this.stopped = stopped;
        }

        @Override
        public void preStart() {
            ready.countDown();
        }

        @Override
        public void postStop() {
            stopped.countDown();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> {}).build();
        }
    }

    public static final class WatchProbe extends AbstractActor {
        private final BlockingQueue<ActorRef> terminated;

        public WatchProbe(BlockingQueue<ActorRef> terminated) {
            this.terminated = terminated;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(WatchCommand.class, command -> {
                        getContext().watch(command.target());
                        getSender().tell("watching", getSelf());
                    })
                    .match(Terminated.class, terminated -> this.terminated.add(terminated.actor()))
                    .build();
        }
    }

    public static final class EventCollector extends AbstractActor {
        private final BlockingQueue<Object> events;

        public EventCollector(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(events::add).build();
        }
    }

    private record WatchCommand(ActorRef target) {
    }
}
