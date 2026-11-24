package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.japi.pf.ReceiveBuilder;
import io.github.espen7.stardust.event.Info;
import io.github.espen7.stardust.event.LogEvent;
import io.github.espen7.stardust.pattern.AskTimeoutException;
import io.github.espen7.stardust.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActorRuntimeTest {
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;

    @AfterEach
    void stopActorSystem() throws Exception {
        if (system != null) system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void askDeliversMessageAndReturnsReply() throws Exception {
        system = ActorSystem.create("ask-reply");
        ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");

        Object response = Patterns.ask(echo, "hello", Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals("hello", response);
    }

    @Test
    void actorLogPublishesStructuredEventWithPekkoMessageTemplate() throws Exception {
        system = ActorSystem.create("logging");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(events::add);
        ActorRef actor = system.actorOf(Props.create(LoggingActor.class), "logger");

        assertEquals("logged", ask(actor, "request-1"));

        Info event = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (event == null && System.nanoTime() < deadline) {
            Object next = events.poll(100, TimeUnit.MILLISECONDS);
            if (next instanceof Info info) event = info;
        }
        assertNotNull(event);
        assertEquals("pekko://logging/user/logger", event.source());
        assertEquals(LoggingActor.class, event.sourceClass());
        assertEquals("handled request-1", event.message());
        assertEquals(LogEvent.Level.INFO, event.level());
    }

    @Test
    void receiveBuilderMatchesTypeAndUsesFallback() throws Exception {
        system = ActorSystem.create("receive-builder");
        ActorRef actor = system.actorOf(Props.create(ReceiveOrderActor.class), "receiver");

        assertEquals("string", ask(actor, "value"));
        assertEquals("fallback", ask(actor, 42));
    }

    @Test
    void becomeAndUnbecomeSwitchAndRestoreBehavior() throws Exception {
        system = ActorSystem.create("behavior-stack");
        ActorRef actor = system.actorOf(Props.create(BehaviorActor.class), "behavior");

        assertEquals("initial", ask(actor, "ping"));
        assertEquals("switched", ask(actor, "switch"));
        assertEquals("initial", ask(actor, "back"));
    }

    @Test
    void askCompletesWithTimeoutWhenActorDoesNotReply() throws Exception {
        system = ActorSystem.create("ask-timeout");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "silent");

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> Patterns.ask(actor, "request", Duration.ofMillis(50))
                        .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));

        assertInstanceOf(AskTimeoutException.class, failure.getCause());
        assertThrows(IllegalArgumentException.class,
                () -> Patterns.ask(actor, "request", Duration.ZERO));
    }

    @Test
    void watcherReceivesTerminatedAfterWatchedActorStops() throws Exception {
        system = ActorSystem.create("death-watch");
        ActorRef watched = system.actorOf(Props.create(SilentActor.class), "watched");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");

        assertEquals("watching", ask(watcher, new Watch(watched)));
        watched.stop();

        assertEquals(watched.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void childFailureRestartsActorInstance() throws Exception {
        system = ActorSystem.create("restart");
        AtomicInteger instances = new AtomicInteger();
        ActorRef child = system.actorOf(Props.create(RestartingActor.class,
                () -> new RestartingActor(instances.incrementAndGet())), "restartable");

        assertEquals(1, ask(child, "instance"));
        child.tell("fail");
        assertEquals(2, ask(child, "instance"));
        assertEquals(2, instances.get());
    }

    @Test
    void terminateCompletesSystemTerminationStage() throws Exception {
        system = ActorSystem.create("terminate");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "actor");
        assertNotNull(actor);

        Terminated terminated = system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertTrue(actor.isTerminated());
        assertTrue(terminated.actor().isTerminated());
        assertTrue(system.getWhenTerminated().toCompletableFuture().isDone());
    }

    private static Object ask(ActorRef actor, Object message) throws Exception {
        return Patterns.ask(actor, message, Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    public static class EchoActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> getSender().tell(message, getSelf())).build();
        }
    }

    public static class LoggingActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                log().info("handled {}", message);
                getSender().tell("logged", getSelf());
            }).build();
        }
    }

    public static class ReceiveOrderActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> getSender().tell("string", getSelf()))
                    .matchAny(message -> getSender().tell("fallback", getSelf()))
                    .build();
        }
    }

    public static class BehaviorActor extends AbstractActor {
        private final Receive switched = receiveBuilder()
                .match(String.class, message -> {
                    if (message.equals("back")) {
                        getContext().unbecome();
                        getSender().tell("initial", getSelf());
                    } else {
                        getSender().tell("switched", getSelf());
                    }
                })
                .build();

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("switch")) {
                            getContext().become(switched);
                            getSender().tell("switched", getSelf());
                        } else {
                            getSender().tell("initial", getSelf());
                        }
                    })
                    .build();
        }
    }

    public static class SilentActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> {}).build();
        }
    }

    private record Watch(ActorRef actor) {
    }

    public static class WatcherActor extends AbstractActor {
        private String terminatedPath;
        private ActorRef waitingRequester;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Watch.class, request -> {
                        getContext().watch(request.actor());
                        getSender().tell("watching", getSelf());
                    })
                    .match(String.class, message -> {
                        if (message.equals("await-termination")) {
                            if (terminatedPath != null) getSender().tell(terminatedPath, getSelf());
                            else waitingRequester = getSender();
                        }
                    })
                    .match(Terminated.class, terminated -> {
                        terminatedPath = terminated.actor().path().toString();
                        if (waitingRequester != null) {
                            waitingRequester.tell(terminatedPath, getSelf());
                            waitingRequester = null;
                        }
                    })
                    .build();
        }
    }

    public static class RestartingActor extends AbstractActor {
        private final int instance;

        public RestartingActor(int instance) {
            this.instance = instance;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("fail")) throw new IllegalStateException("restart");
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }
}
