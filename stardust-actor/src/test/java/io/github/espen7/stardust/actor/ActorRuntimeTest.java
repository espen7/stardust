package io.github.espen7.stardust.actor;

import io.github.espen7.stardust.event.Info;
import io.github.espen7.stardust.event.LogEvent;
import io.github.espen7.stardust.pattern.AskTimeoutException;
import io.github.espen7.stardust.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

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
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "event-subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
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
    void eventStreamUpdatesCachedClassificationsAfterSubscriptionChanges() throws Exception {
        system = ActorSystem.create("event-stream-cache");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        Info event = new Info("test", ActorRuntimeTest.class, "cached");

        system.eventStream().publish(event);
        assertTrue(system.eventStream().subscribe(subscriber, LogEvent.class));
        system.eventStream().publish(event);
        assertEquals(event, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));

        assertTrue(system.eventStream().unsubscribe(subscriber, LogEvent.class));
        system.eventStream().publish(event);
        assertEquals(null, events.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void receiveBuilderMatchesTypeAndUsesFallback() throws Exception {
        system = ActorSystem.create("receive-builder");
        ActorRef actor = system.actorOf(Props.create(ReceiveOrderActor.class), "receiver");

        assertEquals("string", ask(actor, "value"));
        assertEquals("fallback", ask(actor, 42));
    }

    @Test
    void actorContextIsAvailableDuringConstruction() throws Exception {
        system = ActorSystem.create("constructor-context");
        ActorRef actor = system.actorOf(Props.create(ConstructorContextActor.class), "actor");

        assertEquals(actor.path().toString(), ask(actor, "self"));
        assertThrows(ActorInitializationException.class, ConstructorContextActor::new);
    }

    @Test
    void propsSelectsConstructorByArgumentType() throws Exception {
        system = ActorSystem.create("props-constructor");
        ActorRef actor = system.actorOf(Props.create(OverloadedActor.class, "selected"), "actor");

        assertEquals("selected", ask(actor, "value"));
    }

    @Test
    void unhandledTerminatedStopsActorUsingPekkoDefaultDecider() throws Exception {
        system = ActorSystem.create("death-pact");
        AtomicInteger instances = new AtomicInteger();
        ActorRef watched = system.actorOf(Props.create(SilentActor.class), "watched");
        ActorRef watcher = system.actorOf(Props.create(DeathPactActor.class, instances), "watcher");

        assertEquals("watching", ask(watcher, new Watch(watched)));
        watched.stop();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!watcher.isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(watcher.isTerminated());
    }

    @Test
    void defaultDeciderMatchesPekkoExceptionClassification() {
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new ActorInitializationException("initialize")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new ActorKilledException("kill")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                SupervisorStrategy.defaultDecider.apply(new DeathPactException(null)));
        assertEquals(SupervisorStrategy.Directive.Restart,
                SupervisorStrategy.defaultDecider.apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Escalate,
                SupervisorStrategy.defaultDecider.apply(new AssertionError("fatal")));
    }

    @Test
    void childRestartStatsEnforceRetryCountsAndWindowReset() throws Exception {
        system = ActorSystem.create("restart-stats");
        ActorRef child = system.actorOf(Props.create(SilentActor.class), "child");

        io.github.espen7.stardust.actor.dungeon.ChildRestartStats limited =
                new io.github.espen7.stardust.actor.dungeon.ChildRestartStats(child);
        assertTrue(limited.requestRestartPermission(2, null));
        assertTrue(limited.requestRestartPermission(2, null));
        assertFalse(limited.requestRestartPermission(2, null));
        assertFalse(new io.github.espen7.stardust.actor.dungeon.ChildRestartStats(child)
                .requestRestartPermission(0, null));
        assertTrue(new io.github.espen7.stardust.actor.dungeon.ChildRestartStats(child)
                .requestRestartPermission(-1, null));

        io.github.espen7.stardust.actor.dungeon.ChildRestartStats windowed =
                new io.github.espen7.stardust.actor.dungeon.ChildRestartStats(child);
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertFalse(windowed.requestRestartPermission(2, 60_000));

        io.github.espen7.stardust.actor.dungeon.ChildRestartStats expired =
                new io.github.espen7.stardust.actor.dungeon.ChildRestartStats(child, 2,
                        System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
        assertTrue(expired.requestRestartPermission(2, 10));
        assertEquals(1, expired.maxNrOfRetriesCount());
        assertNotEquals(0, expired.restartTimeWindowStartNanos());
    }

    @Test
    void allForOneRestartsEverySiblingAndStopsAllWhenLimitIsExceeded() throws Exception {
        system = ActorSystem.create("all-for-one");
        AtomicInteger firstInstances = new AtomicInteger();
        AtomicInteger secondInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(AllForOneParent.class,
                new AllForOneStrategy(1, Duration.ofSeconds(10), ignored -> SupervisorStrategy.Directive.Restart),
                firstInstances, secondInstances), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertEquals(1, (int) ask(children.get(1), "instance"));

        parent.tell("fail-first");
        awaitInstance(children.get(0), 2);
        awaitInstance(children.get(1), 2);

        parent.tell("fail-first");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (children.stream().anyMatch(child -> !child.isTerminated()) && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(children.stream().allMatch(ActorRef::isTerminated));
    }

    @Test
    void oneForOneRetryLimitStopsOnlyTheFailedChild() throws Exception {
        system = ActorSystem.create("one-for-one-limit");
        AtomicInteger firstInstances = new AtomicInteger();
        AtomicInteger secondInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(1, Duration.ofSeconds(10), ignored -> SupervisorStrategy.Directive.Restart),
                firstInstances, secondInstances), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        parent.tell("fail-first");
        awaitInstance(children.get(0), 2);
        assertEquals(1, (int) ask(children.get(1), "instance"));

        parent.tell("fail-first");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!children.get(0).isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(children.get(0).isTerminated());
        assertFalse(children.get(1).isTerminated());
        assertEquals(1, (int) ask(children.get(1), "instance"));
    }

    @Test
    void resumeKeepsTheFailedChildInstance() throws Exception {
        system = ActorSystem.create("resume-directive");
        AtomicInteger firstInstances = new AtomicInteger();
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume),
                firstInstances, new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertEquals(1, firstInstances.get());
    }

    @Test
    void escalateFailsAndRestartsTheSupervisor() throws Exception {
        system = ActorSystem.create("escalate-directive");
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Escalate),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        java.util.List<ActorRef> restartedChildren = children;
        while (System.nanoTime() < deadline) {
            if (children.stream().allMatch(ActorRef::isTerminated)) {
                restartedChildren = awaitChildren(parent);
                break;
            }
            Thread.yield();
        }
        assertFalse(parent.isTerminated());
        assertTrue(children.stream().allMatch(ActorRef::isTerminated));
        assertEquals(2, restartedChildren.size());
        assertNotEquals(children.get(0), restartedChildren.get(0));
    }

    @Test
    void strategyLoggingCanBeDisabled() throws Exception {
        system = ActorSystem.create("strategy-logging");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume, false),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        assertEquals(1, (int) ask(children.get(0), "instance"));
        assertNull(events.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void resumeFailurePublishesWarningWhenStrategyLoggingIsEnabled() throws Exception {
        system = ActorSystem.create("strategy-warning");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, LogEvent.class);
        ActorRef parent = system.actorOf(Props.create(OneForOneParent.class,
                new OneForOneStrategy(-1, Duration.ofNanos(-1), ignored -> SupervisorStrategy.Directive.Resume),
                new AtomicInteger(), new AtomicInteger()), "parent");

        java.util.List<ActorRef> children = awaitChildren(parent);
        children.get(0).tell("fail");
        Object event = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertInstanceOf(io.github.espen7.stardust.event.Warning.class, event);
        assertEquals(LogEvent.Level.WARNING, ((LogEvent) event).level());
    }

    private static void awaitInstance(ActorRef actor, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            try {
                if ((int) ask(actor, "instance") == expected) return;
            } catch (ExecutionException ignored) {
                Thread.yield();
            }
        }
        fail("Actor did not reach instance " + expected);
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<ActorRef> awaitChildren(ActorRef parent) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            java.util.List<ActorRef> children = (java.util.List<ActorRef>) ask(parent, "children");
            if (children.size() == 2) return children;
            Thread.yield();
        }
        return fail("Parent did not register both children");
    }

    @Test
    void parentRestartRestartsSurvivingChildren() throws Exception {
        system = ActorSystem.create("restart-survivors");
        AtomicInteger instances = new AtomicInteger();
        CountDownLatch restarted = new CountDownLatch(1);
        ActorRef parent = system.actorOf(Props.create(ParentWithSurvivor.class, instances, restarted), "parent");

        assertEquals("spawned", ask(parent, "spawn"));
        parent.tell("fail");

        assertTrue(restarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void parentWaitsForChildrenBeforeTerminating() throws Exception {
        system = ActorSystem.create("wait-for-children");
        CountDownLatch childPostStop = new CountDownLatch(1);
        CountDownLatch allowChildStop = new CountDownLatch(1);
        ActorRef parent = system.actorOf(
                Props.create(ParentWithBlockingChild.class, childPostStop, allowChildStop), "parent");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");

        assertEquals("watching", ask(watcher, new Watch(parent)));
        assertEquals("spawned", ask(parent, "spawn"));
        parent.stop();

        assertTrue(childPostStop.await(WAIT_SECONDS, TimeUnit.SECONDS));
        assertFalse(parent.isTerminated());
        allowChildStop.countDown();
        assertEquals(parent.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void actorPathWithoutAddressReturnsOnlyPathElements() {
        assertEquals("/user/worker", new ActorPath("pekko://system/user/worker").toStringWithoutAddress());
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

    public static class EventSubscriberActor extends AbstractActor {
        private final BlockingQueue<Object> events;

        public EventSubscriberActor(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(events::add).build();
        }
    }

    public static class ConstructorContextActor extends AbstractActor {
        private final String selfPath = getSelf().path().toString();

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, ignored -> getSender().tell(selfPath, getSelf())).build();
        }
    }

    public static class OverloadedActor extends AbstractActor {
        private final String value;

        public OverloadedActor(String value) {
            this.value = value;
        }

        public OverloadedActor(Integer value) {
            this.value = value.toString();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, ignored -> getSender().tell(value, getSelf())).build();
        }
    }

    public static class DeathPactActor extends AbstractActor {
        private final int instance;

        public DeathPactActor(AtomicInteger instances) {
            instance = instances.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Watch.class, request -> {
                        getContext().watch(request.actor());
                        getSender().tell("watching", getSelf());
                    })
                    .match(String.class, message -> {
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class ParentWithSurvivor extends AbstractActor {
        private final AtomicInteger instances;
        private final CountDownLatch restarted;

        public ParentWithSurvivor(AtomicInteger instances, CountDownLatch restarted) {
            this.instances = instances;
            this.restarted = restarted;
        }

        @Override
        public void preRestart(Throwable reason, Object message) throws Exception {
            postStop();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("spawn")) {
                    getContext().actorOf(Props.create(CountedChild.class, instances, restarted), "survivor");
                    getSender().tell("spawned", getSelf());
                } else if (message.equals("fail")) {
                    throw new IllegalStateException("restart parent");
                }
            }).build();
        }
    }

    public static class CountedChild extends AbstractActor {
        public CountedChild(AtomicInteger instances, CountDownLatch restarted) {
            if (instances.incrementAndGet() > 1) restarted.countDown();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    public static class ParentWithBlockingChild extends AbstractActor {
        private final CountDownLatch childPostStop;
        private final CountDownLatch allowChildStop;

        public ParentWithBlockingChild(CountDownLatch childPostStop, CountDownLatch allowChildStop) {
            this.childPostStop = childPostStop;
            this.allowChildStop = allowChildStop;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("spawn")) {
                    getContext().actorOf(Props.create(BlockingStopChild.class, childPostStop, allowChildStop), "child");
                    getSender().tell("spawned", getSelf());
                }
            }).build();
        }
    }

    public static class BlockingStopChild extends AbstractActor {
        private final CountDownLatch childPostStop;
        private final CountDownLatch allowChildStop;

        public BlockingStopChild(CountDownLatch childPostStop, CountDownLatch allowChildStop) {
            this.childPostStop = childPostStop;
            this.allowChildStop = allowChildStop;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }

        @Override
        public void postStop() throws Exception {
            childPostStop.countDown();
            if (!allowChildStop.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test did not release child termination");
            }
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
            return receiveBuilder().matchAny(message -> {
            }).build();
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

    public static class StrategyParent extends AbstractActor {
        private final SupervisorStrategy strategy;
        private final AtomicInteger firstInstances;
        private final AtomicInteger secondInstances;

        public StrategyParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            this.strategy = strategy;
            this.firstInstances = firstInstances;
            this.secondInstances = secondInstances;
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void preStart() {
            getContext().actorOf(Props.create(StrategyChild.class, firstInstances), "first");
            getContext().actorOf(Props.create(StrategyChild.class, secondInstances), "second");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("children")) {
                            getSender().tell(java.util.List.copyOf(getContext().getChildren()), getSelf());
                        } else if (message.equals("fail-first")) {
                            getContext().getChildren().stream()
                                    .filter(child -> child.path().name().equals("first"))
                                    .findFirst().orElseThrow().tell("fail");
                        }
                    })
                    .build();
        }
    }

    public static class AllForOneParent extends StrategyParent {
        public AllForOneParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            super(strategy, firstInstances, secondInstances);
        }
    }

    public static class OneForOneParent extends StrategyParent {
        public OneForOneParent(SupervisorStrategy strategy, AtomicInteger firstInstances, AtomicInteger secondInstances) {
            super(strategy, firstInstances, secondInstances);
        }
    }

    public static class StrategyChild extends AbstractActor {
        private final int instance;

        public StrategyChild(AtomicInteger instances) {
            instance = instances.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("fail")) throw new IllegalStateException("strategy failure");
                if (message.equals("instance")) getSender().tell(instance, getSelf());
            }).build();
        }
    }
}
