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

import io.github.espen7.tapio.dispatch.sysmsg.SystemMessage;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ClassicControlMessageTest {
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;

    @AfterEach
    void stopActorSystem() throws Exception {
        if (system != null) system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void poisonPillStopsActorAndSendsDefaultTerminatedMessage() throws Exception {
        system = ActorSystem.create("poison-pill");
        BlockingQueue<ObservedMessage> events = new LinkedBlockingQueue<>();
        ActorRef watcher = system.actorOf(Props.create(WatchProbe.class, events), "watcher");
        ActorRef target = system.actorOf(Props.create(NoopActor.class), "target");

        assertEquals("watching", ask(watcher, new WatchCommand(target, null, false)));
        target.tell(PoisonPill.getInstance());

        ObservedMessage observed = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(observed);
        assertInstanceOf(Terminated.class, observed.message());
        assertEquals(target, ((Terminated) observed.message()).actor());
        assertEquals(target, observed.sender());
        assertTrue(target.isTerminated());
    }

    @Test
    void killEntersSupervisionAndDefaultStrategyStopsActor() throws Exception {
        system = ActorSystem.create("kill-stop");
        BlockingQueue<ObservedMessage> events = new LinkedBlockingQueue<>();
        ActorRef watcher = system.actorOf(Props.create(WatchProbe.class, events), "watcher");
        ActorRef target = system.actorOf(Props.create(NoopActor.class), "target");

        assertEquals("watching", ask(watcher, new WatchCommand(target, null, false)));
        target.tell(Kill.getInstance());

        ObservedMessage observed = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(observed);
        assertInstanceOf(Terminated.class, observed.message());
        assertTrue(target.isTerminated());
    }

    @Test
    void killCanBeRestartedByCustomSupervisor() throws Exception {
        system = ActorSystem.create("kill-restart");
        AtomicInteger instances = new AtomicInteger();
        BlockingQueue<String> lifecycle = new LinkedBlockingQueue<>();
        SupervisorStrategy strategy = new OneForOneStrategy(-1, Duration.ofNanos(-1),
                ignored -> SupervisorStrategy.Directive.Restart);
        ActorRef parent = system.actorOf(Props.create(SupervisedParent.class,
                strategy, instances, lifecycle), "parent");
        ActorRef child = (ActorRef) ask(parent, "child");

        assertEquals(1, ask(child, "instance"));
        assertEquals("pre-start:1", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        child.tell(Kill.getInstance());
        assertEquals("pre-start:2", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("post-restart:2", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(2, ask(child, "instance"));
        assertFalse(child.isTerminated());
    }

    @Test
    void receiveTimeoutRepeatsAndUsesDeadLettersAsSender() throws Exception {
        system = ActorSystem.create("receive-timeout-repeat");
        BlockingQueue<TimeoutObservation> events = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(TimeoutActor.class, Duration.ofMillis(30), events), "timeout");

        TimeoutObservation first = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        TimeoutObservation second = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(Duration.ofMillis(30), first.timeout());
        assertEquals(system.deadLetters(), first.sender());
        assertEquals(system.deadLetters(), second.sender());
    }

    @Test
    void ordinaryMessageResetsReceiveTimeout() throws Exception {
        system = ActorSystem.create("receive-timeout-reset");
        BlockingQueue<TimeoutObservation> events = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(TimeoutActor.class, Duration.ofMillis(400), events), "timeout");

        assertNotNull(events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("reset", ask(actor, "reset"));
        assertNull(events.poll(Duration.ofMillis(100).toNanos(), TimeUnit.NANOSECONDS));
        assertNotNull(events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void notInfluenceMessageAndCancellationPreserveTimeoutContract() throws Exception {
        system = ActorSystem.create("receive-timeout-markers");
        BlockingQueue<TimeoutObservation> events = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(TimeoutActor.class, Duration.ofMillis(80), events), "timeout");

        assertNotNull(events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("ignored", ask(actor, new NotInfluenceMessage()));
        assertNotNull(events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(Duration.ZERO, ask(actor, "cancel"));
        assertNull(events.poll(Duration.ofMillis(200).toNanos(), TimeUnit.NANOSECONDS));
    }

    @Test
    void restartRetainsReceiveTimeoutAndReschedulesIt() throws Exception {
        system = ActorSystem.create("receive-timeout-restart");
        BlockingQueue<TimeoutObservation> timeouts = new LinkedBlockingQueue<>();
        BlockingQueue<String> lifecycle = new LinkedBlockingQueue<>();
        AtomicInteger instances = new AtomicInteger();
        ActorRef actor = system.actorOf(Props.create(RestartTimeoutActor.class,
                timeouts, lifecycle, instances), "timeout");

        assertNotNull(timeouts.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        actor.tell(new Fail());
        assertEquals("post-restart:2", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(2, ask(actor, "instance"));
        assertNotNull(timeouts.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void watchWithDeliversCustomMessageAndRejectsMixedWatchModes() throws Exception {
        system = ActorSystem.create("watch-with");
        BlockingQueue<ObservedMessage> events = new LinkedBlockingQueue<>();
        ActorRef watcher = system.actorOf(Props.create(WatchProbe.class, events), "watcher");
        ActorRef first = system.actorOf(Props.create(NoopActor.class), "first");
        ActorRef second = system.actorOf(Props.create(NoopActor.class), "second");
        ActorRef third = system.actorOf(Props.create(NoopActor.class), "third");

        assertEquals("watching", ask(watcher, new WatchCommand(first, "first-stopped", true)));
        assertEquals("watching", ask(watcher, new WatchCommand(first, "first-stopped", true)));
        assertEquals(IllegalStateException.class.getName(), ask(watcher, new MixWatchCommand(first, "other")));
        first.tell(PoisonPill.getInstance());

        ObservedMessage observed = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(observed);
        assertEquals("first-stopped", observed.message());
        assertEquals(first, observed.sender());

        assertEquals("watching", ask(watcher, new WatchCommand(second, null, false)));
        assertEquals(IllegalStateException.class.getName(), ask(watcher, new MixWatchCommand(second, "custom")));
        assertEquals("unwatched", ask(watcher, new UnwatchCommand(second)));
        second.tell(PoisonPill.getInstance());
        assertNull(events.poll(Duration.ofMillis(150).toNanos(), TimeUnit.NANOSECONDS));

        assertEquals("watching", ask(watcher, new WatchCommand(third, null, true)));
        third.tell(PoisonPill.getInstance());
        ObservedMessage nullMessage = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(nullMessage);
        assertNull(nullMessage.message());
        assertEquals(third, nullMessage.sender());
    }

    @Test
    void constructorBecomeSurvivesInitialCreationAndRestart() throws Exception {
        system = ActorSystem.create("constructor-become");
        AtomicInteger instances = new AtomicInteger();
        ActorRef actor = system.actorOf(Props.create(ConstructorBecomeActor.class,
                () -> new ConstructorBecomeActor(instances.incrementAndGet())), "actor");

        assertEquals("constructor:1", ask(actor, "state"));
        actor.tell(new Fail());
        assertEquals("constructor:2", ask(actor, "state"));
        assertEquals(2, instances.get());
    }

    @Test
    void constructorAndPreStartFailuresAreWrappedWithActorReference() throws Exception {
        system = ActorSystem.create("creation-failure");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventCollector.class, events), "events");
        system.eventStream().subscribe(subscriber, Error.class);

        ActorRef constructorFailure = system.actorOf(Props.create(FailingConstructorActor.class), "constructor");
        Error constructorError = awaitErrorCause(events, ActorInitializationException.class);
        ActorInitializationException constructorException =
                (ActorInitializationException) constructorError.cause();
        assertEquals(constructorFailure, constructorException.getActor());
        assertTrue(constructorException.getMessage().startsWith(constructorFailure.path() + ": "));
        assertTrue(constructorException.getMessage().contains("constructor-root"));

        ActorRef preStartFailure = system.actorOf(Props.create(FailingPreStartActor.class), "pre-start");
        Error preStartError = awaitErrorCause(events, ActorInitializationException.class);
        ActorInitializationException preStartException = (ActorInitializationException) preStartError.cause();
        assertEquals(preStartFailure, preStartException.getActor());
        assertTrue(preStartException.getMessage().startsWith(preStartFailure.path() + ": "));
        assertTrue(preStartException.getMessage().contains("pre-start-root"));
    }

    @Test
    void creatorCheckedFailureIsWrappedOnceWithActorReference() throws Exception {
        system = ActorSystem.create("creator-failure");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventCollector.class, events), "events");
        system.eventStream().subscribe(subscriber, Error.class);
        Exception failure = new Exception("creator-checked");

        ActorRef actor = system.actorOf(Props.create(NoopActor.class, () -> {
            throw failure;
        }), "creator");
        Error error = awaitErrorCause(events, ActorInitializationException.class);
        ActorInitializationException exception = (ActorInitializationException) error.cause();
        assertEquals(actor, exception.getActor());
        assertSame(failure, exception.getCause());
        assertTrue(exception.getMessage().startsWith(actor.path() + ": "));
        assertTrue(exception.getMessage().contains("creator-checked"));
    }

    @Test
    void preRestartFailureIsLoggedButRestartContinues() throws Exception {
        system = ActorSystem.create("pre-restart-failure");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventCollector.class, events), "events");
        system.eventStream().subscribe(subscriber, Error.class);
        ActorRef actor = system.actorOf(Props.create(RestartLifecycleFailureActor.class,
                "pre", new AtomicInteger(), new AtomicInteger()), "actor");

        actor.tell(new Fail());

        Error error = awaitErrorCause(events, PreRestartException.class);
        PreRestartException exception = (PreRestartException) error.cause();
        assertEquals(actor, exception.getActor());
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals(2, ask(actor, "instance"));
    }

    @Test
    void postRestartFailureIsFatalAndStopsActor() throws Exception {
        system = ActorSystem.create("post-restart-failure");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        BlockingQueue<ObservedMessage> terminated = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventCollector.class, events), "events");
        ActorRef watcher = system.actorOf(Props.create(WatchProbe.class, terminated), "watcher");
        system.eventStream().subscribe(subscriber, Error.class);
        AtomicInteger postStops = new AtomicInteger();
        ActorRef actor = system.actorOf(Props.create(RestartLifecycleFailureActor.class,
                "post", new AtomicInteger(), postStops), "actor");
        assertEquals("watching", ask(watcher, new WatchCommand(actor, null, false)));

        actor.tell(new Fail());

        Error error = awaitErrorCause(events, PostRestartException.class);
        PostRestartException exception = (PostRestartException) error.cause();
        assertEquals(actor, exception.getActor());
        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertNotNull(terminated.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertTrue(actor.isTerminated());
        assertEquals(2, postStops.get());
    }

    @Test
    void restartWaitingForChildrenStashesAndReplaysSystemMessagesInOrder() throws Exception {
        system = ActorSystem.create("restart-system-message-replay");
        CountDownLatch childStopping = new CountDownLatch(1);
        CountDownLatch allowChildStop = new CountDownLatch(1);
        BlockingQueue<String> lifecycle = new LinkedBlockingQueue<>();
        ActorRef parent = system.actorOf(Props.create(BlockingRestartParent.class,
                childStopping, allowChildStop, lifecycle, new AtomicInteger()), "parent");
        assertEquals(1, ask(parent, "instance"));

        parent.tell(new Fail());
        assertTrue(childStopping.await(WAIT_SECONDS, TimeUnit.SECONDS));
        InternalActorRef internalParent = (InternalActorRef) parent;
        internalParent.sendSystemMessage(new SystemMessage.Suspend());
        internalParent.sendSystemMessage(new SystemMessage.Resume(null));
        internalParent.sendSystemMessage(new SystemMessage.Recreate(new IllegalStateException("stashed")));
        allowChildStop.countDown();

        assertEquals("pre-restart:1", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("post-restart:2", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("pre-restart:2", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("post-restart:3", lifecycle.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(3, ask(parent, "instance"));
    }

    private static Object ask(ActorRef actor, Object message) throws Exception {
        return Patterns.ask(actor, message, Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static Error awaitErrorCause(BlockingQueue<Object> events, Class<? extends Throwable> type)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            Object event = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            if (event instanceof Error error && type.isInstance(error.cause())) return error;
        }
        return fail("No Error event with cause " + type.getName());
    }

    public static class NoopActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    public static class EventCollector extends AbstractActor {
        private final BlockingQueue<Object> events;

        public EventCollector(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(events::add).build();
        }
    }

    public static class WatchProbe extends AbstractActor {
        private final BlockingQueue<ObservedMessage> events;

        public WatchProbe(BlockingQueue<ObservedMessage> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(WatchCommand.class, command -> {
                        if (command.custom()) getContext().watchWith(command.target(), command.message());
                        else getContext().watch(command.target());
                        getSender().tell("watching", getSelf());
                    })
                    .match(UnwatchCommand.class, command -> {
                        getContext().unwatch(command.target());
                        getSender().tell("unwatched", getSelf());
                    })
                    .match(MixWatchCommand.class, command -> {
                        try {
                            getContext().watchWith(command.target(), command.message());
                            getSender().tell("no-error", getSelf());
                        } catch (IllegalStateException error) {
                            getSender().tell(error.getClass().getName(), getSelf());
                        }
                    })
                    .matchAny(message -> events.add(new ObservedMessage(message, getSender())))
                    .build();
        }
    }

    public static class SupervisedParent extends AbstractActor {
        private final SupervisorStrategy strategy;
        private final AtomicInteger instances;
        private final BlockingQueue<String> lifecycle;
        private ActorRef child;

        public SupervisedParent(SupervisorStrategy strategy, AtomicInteger instances,
                                BlockingQueue<String> lifecycle) {
            this.strategy = strategy;
            this.instances = instances;
            this.lifecycle = lifecycle;
        }

        @Override
        public SupervisorStrategy supervisorStrategy() {
            return strategy;
        }

        @Override
        public void preStart() {
            child = getContext().actorOf(Props.create(CountedActor.class,
                    () -> new CountedActor(instances.incrementAndGet(), lifecycle)), "child");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("child")) getSender().tell(child, getSelf());
            }).build();
        }
    }

    public static class CountedActor extends AbstractActor {
        private final int instance;
        private final BlockingQueue<String> lifecycle;

        public CountedActor(int instance, BlockingQueue<String> lifecycle) {
            this.instance = instance;
            this.lifecycle = lifecycle;
        }

        @Override
        public void preStart() {
            lifecycle.add("pre-start:" + instance);
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            super.postRestart(reason);
            lifecycle.add("post-restart:" + instance);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class, message -> {
                if (message.equals("instance")) getSender().tell(instance, getSelf());
            }).build();
        }
    }

    public static class TimeoutActor extends AbstractActor {
        private final Duration timeout;
        private final BlockingQueue<TimeoutObservation> events;

        public TimeoutActor(Duration timeout, BlockingQueue<TimeoutObservation> events) {
            this.timeout = timeout;
            this.events = events;
        }

        @Override
        public void preStart() {
            getContext().setReceiveTimeout(timeout);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReceiveTimeout.class, message -> events.add(
                            new TimeoutObservation(message.timeout(), getSender())))
                    .match(String.class, message -> {
                        if (message.equals("reset")) getSender().tell("reset", getSelf());
                        else if (message.equals("cancel")) {
                            getContext().cancelReceiveTimeout();
                            getSender().tell(getContext().getReceiveTimeout(), getSelf());
                        }
                    })
                    .match(NotInfluenceMessage.class, message -> getSender().tell("ignored", getSelf()))
                    .build();
        }
    }

    public static class RestartTimeoutActor extends AbstractActor {
        private final BlockingQueue<TimeoutObservation> timeouts;
        private final BlockingQueue<String> lifecycle;
        private final int instance;

        public RestartTimeoutActor(BlockingQueue<TimeoutObservation> timeouts, BlockingQueue<String> lifecycle,
                                   AtomicInteger instances) {
            this.timeouts = timeouts;
            this.lifecycle = lifecycle;
            this.instance = instances.incrementAndGet();
        }

        @Override
        public void preStart() {
            if (getContext().getReceiveTimeout().isZero()) {
                getContext().setReceiveTimeout(Duration.ofMillis(50));
            }
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            super.postRestart(reason);
            lifecycle.add("post-restart:" + instance);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ReceiveTimeout.class, message -> timeouts.add(
                            new TimeoutObservation(message.timeout(), getSender())))
                    .match(Fail.class, message -> {
                        throw new IllegalStateException("restart timeout actor");
                    })
                    .match(String.class, message -> {
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class ConstructorBecomeActor extends AbstractActor {
        private final int instance;

        public ConstructorBecomeActor(int instance) {
            this.instance = instance;
            getContext().become(receiveBuilder()
                    .match(String.class, message -> getSender().tell("constructor:" + instance, getSelf()))
                    .match(Fail.class, message -> {
                        throw new IllegalStateException("restart constructor behavior");
                    })
                    .build());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> getSender().tell("receive", getSelf())).build();
        }
    }

    public static class FailingConstructorActor extends AbstractActor {
        public FailingConstructorActor() {
            throw new IllegalStateException("constructor-root");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    public static class FailingPreStartActor extends AbstractActor {
        @Override
        public void preStart() {
            throw new IllegalStateException("pre-start-root");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    public static class RestartLifecycleFailureActor extends AbstractActor {
        private final String failureStage;
        private final int instance;
        private final AtomicInteger postStops;

        public RestartLifecycleFailureActor(String failureStage, AtomicInteger instances, AtomicInteger postStops) {
            this.failureStage = failureStage;
            this.instance = instances.incrementAndGet();
            this.postStops = postStops;
        }

        @Override
        public void preRestart(Throwable reason, Object message) throws Exception {
            if (failureStage.equals("pre")) throw new IllegalStateException("pre-restart-failure");
            super.preRestart(reason, message);
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            if (failureStage.equals("post")) throw new IllegalStateException("post-restart-failure");
            super.postRestart(reason);
        }

        @Override
        public void postStop() {
            postStops.incrementAndGet();
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Fail.class, message -> {
                        throw new IllegalStateException("receive-failure");
                    })
                    .match(String.class, message -> {
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class BlockingRestartParent extends AbstractActor {
        private final CountDownLatch childStopping;
        private final CountDownLatch allowChildStop;
        private final BlockingQueue<String> lifecycle;
        private final int instance;

        public BlockingRestartParent(CountDownLatch childStopping, CountDownLatch allowChildStop,
                                     BlockingQueue<String> lifecycle, AtomicInteger instances) {
            this.childStopping = childStopping;
            this.allowChildStop = allowChildStop;
            this.lifecycle = lifecycle;
            this.instance = instances.incrementAndGet();
        }

        @Override
        public void preStart() {
            getContext().actorOf(Props.create(BlockingChild.class, childStopping, allowChildStop), "child");
        }

        @Override
        public void preRestart(Throwable reason, Object message) throws Exception {
            lifecycle.add("pre-restart:" + instance);
            super.preRestart(reason, message);
        }

        @Override
        public void postRestart(Throwable reason) throws Exception {
            super.postRestart(reason);
            lifecycle.add("post-restart:" + instance);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Fail.class, message -> {
                        throw new IllegalStateException("restart parent");
                    })
                    .match(String.class, message -> {
                        if (message.equals("instance")) getSender().tell(instance, getSelf());
                    })
                    .build();
        }
    }

    public static class BlockingChild extends AbstractActor {
        private final CountDownLatch stopping;
        private final CountDownLatch allowStop;

        public BlockingChild(CountDownLatch stopping, CountDownLatch allowStop) {
            this.stopping = stopping;
            this.allowStop = allowStop;
        }

        @Override
        public void postStop() throws Exception {
            stopping.countDown();
            if (!allowStop.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("child stop was not released");
            }
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }

    private record Fail() {
    }

    private record NotInfluenceMessage() implements NotInfluenceReceiveTimeout {
    }

    private record WatchCommand(ActorRef target, Object message, boolean custom) {
    }

    private record UnwatchCommand(ActorRef target) {
    }

    private record MixWatchCommand(ActorRef target, Object message) {
    }

    private record ObservedMessage(Object message, ActorRef sender) {
    }

    private record TimeoutObservation(Duration timeout, ActorRef sender) {
    }
}
