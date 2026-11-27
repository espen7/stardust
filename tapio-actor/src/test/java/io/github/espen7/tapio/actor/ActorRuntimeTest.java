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
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.dispatch.Dispatcher;
import io.github.espen7.tapio.dispatch.DispatcherTestSupport;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.Mailbox;
import io.github.espen7.tapio.dispatch.MailboxType;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.dispatch.MessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMailbox;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.event.Info;
import io.github.espen7.tapio.event.LogEvent;
import io.github.espen7.tapio.japi.pf.ReceiveBuilder;
import io.github.espen7.tapio.pattern.AskTimeoutException;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class ActorRuntimeTest {
    private static final long WAIT_SECONDS = 3;

    private ActorSystem system;

    @Test
    void settingsAndDefaultDispatcherUseApplicationConfig() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.actor.default-dispatcher.throughput = 11
                tapio.loglevel = "WARNING"
                tapio.actor.loglevel = "DEBUG"
                tapio.log-dead-letters = off
                """);
        system = ActorSystem.create("configured-dispatcher", config);

        assertEquals("configured-dispatcher", system.settings().name());
        assertEquals(0, system.settings().LogDeadLetters);
        assertEquals(LogEvent.Level.WARNING, system.eventStream().logLevel());
        assertSame(system.dispatcher(), system.dispatchers().defaultGlobalDispatcher());
        assertEquals(11, system.dispatcher().throughput());
        assertEquals("reply", ask(system.actorOf(Props.create(EchoActor.class), "echo"), "reply"));
    }

    @Test
    void settingsRetainLegacyLogLevelAsFallback() {
        system = ActorSystem.create("legacy-loglevel", ConfigFactory.parseString("tapio.actor.loglevel = DEBUG"));

        assertEquals(LogEvent.Level.DEBUG, system.eventStream().logLevel());
        assertEquals(5, system.dispatcher().throughput());
    }

    @Test
    void settingsRejectConflictingShutdownConfig() {
        Config config = ConfigFactory.parseString("tapio.coordinated-shutdown.terminate-actor-system = off");

        ConfigurationException error = assertThrows(ConfigurationException.class,
                () -> ActorSystem.create("invalid-shutdown", config));
        assertTrue(error.getMessage().contains("run-by-actor-system-terminate=on"));
    }

    @Test
    void dispatchersLookupResolvesAliasesAndInheritsDefaultConfig() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.actor.default-dispatcher.throughput = 9
                dispatchers.shared {}
                dispatchers.alias = "dispatchers.shared"
                dispatchers.next = "dispatchers.alias"
                dispatchers.loop = "dispatchers.loop"
                dispatchers.invalid = 1
                """);
        system = ActorSystem.create("dispatcher-lookup", config);
        Dispatchers dispatchers = system.dispatchers();

        assertTrue(dispatchers.hasDispatcher("dispatchers.alias"));
        assertFalse(dispatchers.hasDispatcher("dispatchers.missing"));
        MessageDispatcher shared = dispatchers.lookup("dispatchers.next");
        assertEquals(9, shared.throughput());
        assertNotSame(system.dispatcher(), shared);
        assertSame(shared, dispatchers.lookup("dispatchers.shared"));
        assertSame(shared, dispatchers.lookup("dispatchers.alias"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.missing"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.loop"));
        assertThrows(ConfigurationException.class, () -> dispatchers.lookup("dispatchers.invalid"));
        assertSame(system.dispatcher(), dispatchers.lookup(Dispatchers.DefaultDispatcherId));
        system.close();
        assertSame(shared, dispatchers.lookup("dispatchers.shared"));
    }

    @Test
    void internalDispatcherIsSharedByGuardiansAndSeparateFromUserActors() throws Exception {
        system = ActorSystem.create("internal-dispatcher");
        MessageDispatcher internal = system.dispatchers().internalDispatcher();
        LocalActorRefProvider provider = (LocalActorRefProvider) ((ActorSystemImpl) system).provider();

        assertNotSame(system.dispatcher(), internal);
        assertSame(internal, system.dispatchers().lookup("tapio.actor.internal-dispatcher"));
        assertSame(internal, provider.rootGuardian().underlying().dispatcher());
        assertSame(internal, provider.guardian().underlying().dispatcher());
        assertSame(internal, provider.systemGuardian().underlying().dispatcher());

        ActorRef user = system.actorOf(Props.create(EchoActor.class), "user-actor");
        assertEquals("ready", ask(user, "ready"));
        ActorRefWithCell started = assertInstanceOf(ActorRefWithCell.class, provider.resolveActorRef(user.path()));
        ActorCell cell = (ActorCell) started.underlying();
        assertSame(system.dispatcher(), cell.dispatcher());
    }

    @Test
    void internalDispatcherCanBeConfiguredAsAlias() {
        system = ActorSystem.create("internal-alias", ConfigFactory.parseString("""
                tapio.actor.internal-dispatcher = "dispatchers.internal-target"
                dispatchers.internal-target {
                  type = "Dispatcher"
                  throughput = 3
                  executor = "thread-pool-executor"
                  thread-pool-executor.fixed-pool-size = 1
                }
                """));
        MessageDispatcher internal = system.dispatchers().internalDispatcher();
        LocalActorRefProvider provider = (LocalActorRefProvider) ((ActorSystemImpl) system).provider();

        assertSame(internal, system.dispatchers().lookup("dispatchers.internal-target"));
        assertEquals(3, internal.throughput());
        assertSame(internal, provider.rootGuardian().underlying().dispatcher());
        assertSame(internal, provider.guardian().underlying().dispatcher());
        assertSame(internal, provider.systemGuardian().underlying().dispatcher());
    }

    @Test
    void configuredDispatcherConstructorUsesEffectiveApplicationConfig() throws Exception {
        system = ActorSystem.create("public-dispatcher", ConfigFactory.parseString("""
                tapio.actor.default-dispatcher.throughput = 13
                tapio.actor.default-dispatcher.default-executor.fallback = "thread-pool-executor"
                tapio.actor.default-dispatcher.thread-pool-executor.fixed-pool-size = 1
                """));
        Dispatcher dispatcher = DispatcherTestSupport.configuredDispatcher(system);
        CountDownLatch ran = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();
        try {
            assertEquals(13, dispatcher.throughput());
            dispatcher.execute(() -> {
                threadName.set(Thread.currentThread().getName());
                ran.countDown();
            });
            assertTrue(ran.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(threadName.get().startsWith("public-dispatcher-" + Dispatchers.DefaultDispatcherId + "-"));
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    void namedDispatcherUsesConfiguredExecutorAndThreadName() throws Exception {
        system = ActorSystem.create("named-dispatcher", ConfigFactory.parseString("""
                dispatchers.named {
                  executor = "thread-pool-executor"
                  thread-pool-executor.fixed-pool-size = 1
                }
                """));
        ActorRef actor = system.actorOf(Props.create(ThreadNameActor.class)
                .withDispatcher("dispatchers.named"), "actor");

        String first = (String) ask(actor, "first");
        String second = (String) ask(actor, "second");
        assertEquals(first, second);
        assertTrue(first.startsWith("named-dispatcher-dispatchers.named-"));
    }

    @Test
    void actorSystemNameUsesPekkoNameRules() {
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("-leading"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("with space"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create("with/slash"));
        assertThrows(IllegalArgumentException.class, () -> ActorSystem.create(""));

        ActorSystem valid = ActorSystem.create("a-system_2");
        assertDoesNotThrow(() -> valid.terminate().toCompletableFuture().join());
    }

    @Test
    void systemActorOfUsesSystemGuardianPath() throws Exception {
        system = ActorSystem.create("system-actors");
        ActorRef actor = ((ExtendedActorSystem) system).systemActorOf(Props.create(EchoActor.class), "service");

        assertEquals("/system/service", actor.path().toStringWithoutAddress());
        assertEquals("service", actor.path().name());
        assertEquals("reply", ask(actor, "reply"));
    }

    @Test
    void coordinatedShutdownRunsRegisteredTaskBeforeTermination() throws Exception {
        system = ActorSystem.create("coordinated-shutdown");
        BlockingQueue<Object> completed = new LinkedBlockingQueue<>();
        CoordinatedShutdown.get(system).addTask(
                CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
                "test-task",
                () -> {
                    completed.add("done");
                    return CompletableFuture.completedFuture(Done.done());
                });

        system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals("done", completed.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(CoordinatedShutdown.ActorSystemTerminateReason.class,
                CoordinatedShutdown.get(system).shutdownReason().orElseThrow().getClass());
    }

    @Test
    void topLevelActorOfAttachesChildToUserGuardian() throws Exception {
        system = ActorSystem.create("attach-child");
        ActorRef actor = system.actorOf(Props.create(EchoActor.class), "echo");

        assertEquals("/user/echo", actor.path().toStringWithoutAddress());
        assertEquals("echo", actor.path().name());
        assertEquals("hello", ask(actor, "hello"));
    }

    @Test
    void rootGuardianResolvesBuiltInTopLevelPaths() {
        system = ActorSystem.create("root-guardian");
        LocalActorRefProvider provider = (LocalActorRefProvider) ((ActorSystemImpl) system).provider();
        LocalActorRef root = provider.rootGuardian();
        LocalActorRef user = provider.guardian();
        LocalActorRef systemGuardian = provider.systemGuardian();
        VirtualPathContainer temp = (VirtualPathContainer) provider.tempContainer();

        assertEquals("tapio://root-guardian/", root.path().toString());
        assertEquals("/", root.path().name());
        assertEquals(ActorPath.UndefinedUid, root.path().uid());
        assertSame(root.path(), root.path().parent());
        assertSame(root, root.getParent());
        assertEquals("/bubble-walker", root.underlying().parent().path().toStringWithoutAddress());
        assertSame(Nobody.INSTANCE, root.getSingleChild("bubble-walker"));
        assertSame(root, user.getParent());
        assertSame(root, systemGuardian.getParent());
        assertSame(root, temp.getParent());
        assertSame(user, root.getSingleChild("user"));
        assertSame(user, root.getSingleChild("user#" + user.path().uid()));
        assertSame(systemGuardian, root.getSingleChild("system"));
        assertSame(temp, root.getSingleChild("temp"));
        assertSame(system.deadLetters(), root.getSingleChild("deadLetters"));
        assertSame(Nobody.INSTANCE, root.getSingleChild("remote"));
        assertSame(root.path(), user.path().parent());
        assertSame(root.path(), systemGuardian.path().parent());
        assertSame(root.path(), temp.path().parent());
        assertSame(root.path(), system.deadLetters().path().parent());
    }

    @Test
    void actorRefFactoryCreatesAndStopsActorsFromSystemAndContext() throws Exception {
        system = ActorSystem.create("factory");
        ActorRefFactory factory = system;
        ActorRef parent = factory.actorOf(Props.create(FactoryProbe.class));
        assertEquals("/user/$a", parent.path().toStringWithoutAddress());
        assertSame(system.dispatcher(), factory.dispatcher());
        ActorRef child = (ActorRef) ask(parent, "create-child");
        assertEquals("/user/$a/$a", child.path().toStringWithoutAddress());
        assertEquals("ready", ask(child, "ready"));
        assertThrows(InvalidActorNameException.class, () -> factory.actorOf(Props.create(EchoActor.class), "$a"));
        assertThrows(InvalidActorNameException.class, () -> factory.actorOf(Props.create(EchoActor.class), "a#b"));
        ActorRef service = ((ExtendedActorSystem) system).systemActorOf(Props.create(EchoActor.class), "service");

        Props watcherProps = Props.create(WatcherActor.class);
        ActorRef childWatcher = factory.actorOf(watcherProps, "child-watcher");
        ActorRef parentWatcher = factory.actorOf(watcherProps, "parent-watcher");
        ActorRef serviceWatcher = factory.actorOf(watcherProps, "service-watcher");
        assertThrows(InvalidActorNameException.class, () -> factory.actorOf(watcherProps, "child-watcher"));
        assertEquals("watching", ask(childWatcher, new Watch(child)));
        assertEquals("watching", ask(parentWatcher, new Watch(parent)));
        assertEquals("watching", ask(serviceWatcher, new Watch(service)));
        assertEquals("stopping", ask(parent, "stop-child"));
        assertEquals(child.path().toString(), ask(childWatcher, "await-termination"));
        factory.stop(parent);
        assertEquals(parent.path().toString(), ask(parentWatcher, "await-termination"));
        factory.stop(service);
        assertEquals(service.path().toString(), ask(serviceWatcher, "await-termination"));
    }

    @Test
    void providerResolvesLocalPathsAndRetainsMissingRecipient() throws Exception {
        system = ActorSystem.create("provider");
        ActorRefProvider provider = ((ActorSystemImpl) system).provider();
        Address local = new Address(ActorPath.Scheme, "provider");
        Address foreign = new Address(ActorPath.Scheme, "foreign");
        assertEquals(local, provider.getDefaultAddress());
        assertEquals(local, provider.rootPath().address());
        assertEquals(local, provider.getExternalAddressFor(local).orElseThrow());
        assertTrue(provider.getExternalAddressFor(foreign).isEmpty());
        assertSame(provider.rootGuardian(), provider.rootGuardianAt(local));
        assertSame(system.deadLetters(), provider.rootGuardianAt(foreign));
        assertSame(system.deadLetters(), provider.resolveActorRef("tapio://foreign/user/worker"));
        assertSame(system.deadLetters(), provider.resolveActorRef("/user/worker"));
        assertSame(provider.rootGuardian(), provider.resolveActorRef("tapio://provider/"));
        assertSame(provider.tempContainer(), provider.resolveActorRef("tapio://provider/temp"));

        ActorRef actor = system.actorOf(Props.create(EchoActor.class), "worker");
        assertSame(actor, provider.resolveActorRef(actor.path()));
        String pathWithUid = actor.path() + "#" + actor.path().uid();
        assertSame(actor, provider.resolveActorRef(pathWithUid));
        int otherUid = actor.path().uid() == 1 ? 2 : 1;
        ActorRef missing = provider.resolveActorRef(actor.path() + "#" + otherUid);
        assertInstanceOf(EmptyLocalActorRef.class, missing);
        assertEquals(actor.path(), missing.path());
        assertEquals(otherUid, missing.path().uid());

        ActorRef ignore = provider.ignoreRef();
        assertEquals("tapio://local/ignore", ignore.path().toString());
        ignore.tell("ignored", ActorRef.noSender);
        assertFalse(((InternalActorRef) ignore).isTerminated());
    }

    @Test
    void providerUsesExplicitDeployWhenLookupIsDisabled() throws Exception {
        system = ActorSystem.create("provider-deploy", ConfigFactory.parseString("""
                tapio.actor.deployment./worker.dispatcher = "missing-dispatcher"
                """));
        ActorSystemImpl systemImpl = (ActorSystemImpl) system;
        ActorRefProvider provider = systemImpl.provider();
        LocalActorRef guardian = provider.guardian();
        ActorPath path = new ChildActorPath(guardian.path(), "worker", 17);
        Props props = Props.create(EchoActor.class);
        Optional<Deploy> deploy = Optional.of(new Deploy("/worker", Dispatchers.DefaultDispatcherId, ""));

        assertThrows(ConfigurationException.class,
                () -> provider.actorOf(systemImpl, props, guardian, path, false, deploy, true, false));
        guardian.underlying().reserveChild("worker");
        InternalActorRef actor = provider.actorOf(systemImpl, props, guardian, path, false, deploy, false, false);
        guardian.underlying().initChild(actor);
        actor.start();
        assertSame(path, actor.path());
        assertEquals("ready", ask(actor, "ready"));
    }

    @Test
    void actorSelectionResolvesFromSystemAndContext() throws Exception {
        system = ActorSystem.create("selection");
        ActorRef parent = system.actorOf(Props.create(FactoryProbe.class), "parent");
        ActorRef child = (ActorRef) ask(parent, "create-child");
        Duration timeout = Duration.ofSeconds(1);

        assertSame(child, system.actorSelection(child.path()).resolveOne(timeout)
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertSame(child, system.actorSelection(child.path().toString()).resolveOne(timeout)
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("hello", Patterns.ask(system.actorSelection("/user/parent/*"), "hello", timeout)
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("relative", ask(parent, "select-child"));
        assertSame(parent, ActorSelection.apply(child, "..").resolveOne(timeout)
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertSame(child, ActorSelection.apply(parent, "$?").resolveOne(timeout)
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        ExecutionException missing = assertThrows(ExecutionException.class, () ->
                system.actorSelection("tapio://selection/user/missing").resolveOne(timeout)
                        .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertInstanceOf(ActorNotFound.class, missing.getCause());
    }

    @Test
    void wildcardSelectionFansOutAndPublishesMissingPayloadAsDeadLetter() throws Exception {
        system = ActorSystem.create("selection-fanout");
        ActorRef first = system.actorOf(Props.create(EchoActor.class), "echo-a");
        ActorRef second = system.actorOf(Props.create(EchoActor.class), "echo-b");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef observer = system.actorOf(Props.create(EventSubscriberActor.class, events), "observer");
        system.eventStream().subscribe(observer, DeadLetter.class);

        system.actorSelection("/user/echo-*").tell(new Identify("echoes"), observer);
        ActorIdentity one = assertInstanceOf(ActorIdentity.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        ActorIdentity two = assertInstanceOf(ActorIdentity.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(java.util.Set.of(first, second), java.util.Set.of(one.ref().orElseThrow(), two.ref().orElseThrow()));

        system.actorSelection("/user/echo-missing").tell("payload", observer);
        DeadLetter missing = assertInstanceOf(DeadLetter.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("payload", missing.message());
        assertSame(observer, missing.sender());
        assertEquals("/user/echo-missing", missing.recipient().path().toStringWithoutAddress());
    }

    @Test
    void asynchronousAttachmentBuffersMessagesUntilSupervisorActivatesTheCell() throws Exception {
        system = ActorSystem.create("repointable", ConfigFactory.parseString("""
                activation-mailbox.mailbox-type = "io.github.espen7.tapio.actor.ActorRuntimeTest$ActivationMailbox"
                """));
        BlockingQueue<Object> replies = new LinkedBlockingQueue<>();
        ActorRef receiver = system.actorOf(Props.create(EventSubscriberActor.class, replies), "receiver");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        assertInstanceOf(ActorIdentity.class, ask(receiver, new Identify("ready")));
        assertInstanceOf(ActorIdentity.class, ask(watcher, new Identify("ready")));
        ActivationMailbox.entered = new CountDownLatch(1);
        ActivationMailbox.release = new CountDownLatch(1);
        ActorRef actor = system.actorOf(Props.create(EchoActor.class).withMailbox("activation-mailbox"), "buffered");
        try {
            assertTrue(ActivationMailbox.entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
            RepointableActorRef repointable = assertInstanceOf(RepointableActorRef.class, actor);
            assertInstanceOf(UnstartedCell.class, repointable.underlying());
            assertEquals("watching", ask(watcher, new Watch(actor)));
            actor.tell("first", receiver);
            actor.tell("second", receiver);
            assertTrue(replies.isEmpty());
        } finally {
            ActivationMailbox.release.countDown();
        }
        assertEquals("first", replies.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals("second", replies.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        system.stop(actor);
        assertEquals(actor.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void actorRefOrderingUsesThePathAndIncarnation() throws Exception {
        ActorPath parent = ActorPath.fromString("tapio://ordering/user");
        ActorRef first = new EmptyLocalActorRef(null, new ChildActorPath(parent, "worker", -1), null);
        ActorRef second = new EmptyLocalActorRef(null, new ChildActorPath(parent, "worker", 1), null);
        ActorRef same = new EmptyLocalActorRef(null, new ChildActorPath(parent, "worker", -1), null);
        assertEquals(first.path(), second.path());
        assertTrue(first.compareTo(second) < 0);
        assertTrue(second.compareTo(first) > 0);
        assertEquals(0, first.compareTo(same));

        assertTrue(ActorPath.fromString("tapio://ordering/user/z/first")
                .compareTo(ActorPath.fromString("tapio://ordering/user/a/last")) < 0);
        assertTrue(ActorPath.fromString("tapio://ordering/").compareTo(parent) > 0);
    }

    @Test
    void receiveOrElsePreservesFirstMatchAndLeavesUnmatchedInputUndefined() throws Exception {
        List<String> received = new ArrayList<>();
        AtomicInteger predicateCalls = new AtomicInteger();
        var primary = ReceiveBuilder.create()
                .match(String.class, value -> {
                    predicateCalls.incrementAndGet();
                    return value.startsWith("a");
                }, value -> received.add("first:" + value))
                .build();
        var fallback = ReceiveBuilder.create()
                .match(String.class, value -> received.add("second:" + value))
                .build();
        var combined = primary.orElse(fallback).onMessage();
        assertFalse(combined.isDefinedAt(42));
        combined.applyOrElse("apple", value -> fail("expected the first match"));
        combined.applyOrElse("berry", value -> fail("expected the fallback receive"));
        assertEquals(List.of("first:apple", "second:berry"), received);
        assertEquals(2, predicateCalls.get());
    }

    @Test
    void localProviderReusesSerializationInformation() {
        system = ActorSystem.create("serialization-info");
        ActorRefProvider provider = ((ActorSystemImpl) system).provider();
        var info = provider.serializationInformation();
        assertSame(info, provider.serializationInformation());
        assertSame(system, info.system());
        assertEquals(provider.getDefaultAddress(), info.address());
    }

    @Test
    void asynchronousMailboxCreationFailureTerminatesTheActor() throws Exception {
        system = ActorSystem.create("repointable-failure", ConfigFactory.parseString("""
                failing-mailbox.mailbox-type = "io.github.espen7.tapio.actor.ActorRuntimeTest$FailingMailbox"
                """));
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "events");
        system.eventStream().subscribe(subscriber, Error.class);
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        ActorRef actor = system.actorOf(Props.create(EchoActor.class).withMailbox("failing-mailbox"), "failed");
        Error error = assertInstanceOf(Error.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        ActorInitializationException exception =
                assertInstanceOf(ActorInitializationException.class, error.cause());
        assertEquals(actor, exception.getActor());
        assertEquals(actor.path() + ": failure while creating ActorCell", exception.getMessage());
        assertEquals("mailbox creation failure", exception.getCause().getMessage());
        assertEquals("watching", ask(watcher, new Watch(actor)));
        assertEquals(actor.path().toString(), ask(watcher, "await-termination"));
        assertTrue(((InternalActorRef) actor).isTerminated());
        assertEquals("ready", ask(system.actorOf(Props.create(EchoActor.class), "healthy"), "ready"));
    }

    public static final class ThreadNameActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().match(String.class,
                    message -> getSender().tell(Thread.currentThread().getName(), getSelf())).build();
        }
    }

    public static final class FailingMailbox implements MailboxType {
        public FailingMailbox(ActorSystem.Settings settings, Config config) {
        }

        @Override
        public MessageQueue create(ActorRef owner, ActorSystem system) {
            throw new IllegalStateException("mailbox creation failure");
        }
    }

    public static final class ActivationMailbox implements MailboxType {
        private static volatile CountDownLatch entered;
        private static volatile CountDownLatch release;

        public ActivationMailbox(ActorSystem.Settings settings, Config config) {
        }

        @Override
        public MessageQueue create(ActorRef owner, ActorSystem system) {
            entered.countDown();
            try {
                if (!release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("activation gate was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return new UnboundedMailbox.MessageQueue();
        }
    }

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
    void identifyRepliesForLiveAndDeadLetterReferences() throws Exception {
        system = ActorSystem.create("identify");
        ActorRef actor = system.actorOf(Props.create(SilentActor.class), "actor");
        ActorIdentity live = (ActorIdentity) ask(actor, new Identify("live"));
        assertEquals("live", live.correlationId());
        assertEquals(actor, live.getActorRef().orElseThrow());

        BlockingQueue<Object> replies = new LinkedBlockingQueue<>();
        ActorRef probe = system.actorOf(Props.create(EventSubscriberActor.class, replies), "probe");
        system.deadLetters().tell(new Identify("missing"), probe);
        ActorIdentity missing = (ActorIdentity) replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(missing);
        assertEquals("missing", missing.correlationId());
        assertTrue(missing.getActorRef().isEmpty());
    }

    @Test
    void schedulerReportsSubmissionFailureToExecutionContext() throws Exception {
        system = ActorSystem.create("scheduler-report");
        BlockingQueue<Throwable> failures = new LinkedBlockingQueue<>();
        RuntimeException rejection = new RuntimeException("rejected");
        io.github.espen7.tapio.dispatch.ExecutionContext executionContext =
                new io.github.espen7.tapio.dispatch.ExecutionContext() {
                    @Override
                    public void execute(Runnable runnable) {
                        throw rejection;
                    }

                    @Override
                    public void reportFailure(Throwable cause) {
                        failures.add(cause);
                    }
                };
        system.scheduler().scheduleOnce(Duration.ofMillis(10), () -> fail("must not execute"), executionContext);
        assertSame(rejection, failures.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void deadLetterListenerPublishesInfoWithRecipientSource() throws Exception {
        system = ActorSystem.create("dead-letter-info", ConfigFactory.parseString("""
                tapio.log-dead-letters = 1
                tapio.log-dead-letters-suspend-duration = infinite
                """));
        BlockingQueue<Object> logs = new LinkedBlockingQueue<>();
        ActorRef probe = system.actorOf(Props.create(EventSubscriberActor.class, logs), "log-probe");
        system.eventStream().subscribe(probe, Info.class);
        ActorRef recipient = system.actorOf(Props.create(SilentActor.class), "recipient");
        ActorRef listener = system.actorOf(Props.create(io.github.espen7.tapio.event.DeadLetterListener.class),
                "test-listener");
        listener.tell(new DeadLetter("missing", system.deadLetters(), recipient), ActorRef.noSender);

        Info info = (Info) logs.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(info);
        assertEquals(recipient.path().toString(), info.source());
        assertEquals(recipient.getClass(), info.sourceClass());
        assertTrue(info.message().contains("Message [java.lang.String] to " + recipient + " was not delivered."));
        assertTrue(info.message().contains("[1] dead letters encountered, no more dead letters will be logged."));
        assertNotNull(info.marker());
        assertEquals("tapioDeadLetter", info.marker().name());
        assertEquals("java.lang.String", info.marker().properties().get("tapioMessageClass"));
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
        assertEquals("tapio://logging/user/logger", event.source());
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
        system.stop(watched);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!((InternalActorRef) watcher).isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(((InternalActorRef) watcher).isTerminated());
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
    void strategyConstructorsUsePekkoDefaults() {
        Duration window = Duration.ofSeconds(1);
        OneForOneStrategy oneForOne = new OneForOneStrategy(2, window, SupervisorStrategy.defaultDecider);
        assertEquals(2, oneForOne.maxNrOfRetries());
        assertEquals(window, oneForOne.withinTimeRange());
        assertTrue(oneForOne.loggingEnabled());
        assertEquals(SupervisorStrategy.Directive.Restart,
                oneForOne.decider().apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                oneForOne.decider().apply(new ActorKilledException("kill")));

        AllForOneStrategy allForOne = new AllForOneStrategy(3, window, SupervisorStrategy.defaultDecider);
        assertEquals(3, allForOne.maxNrOfRetries());
        assertEquals(window, allForOne.withinTimeRange());
        assertTrue(allForOne.loggingEnabled());
        assertEquals(SupervisorStrategy.Directive.Restart,
                allForOne.decider().apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Stop,
                allForOne.decider().apply(new ActorKilledException("kill")));

        java.util.List<Class<? extends Throwable>> trapExit = new java.util.ArrayList<>();
        trapExit.add(IllegalStateException.class);
        OneForOneStrategy trappingOneForOne = new OneForOneStrategy(2, window, trapExit);
        AllForOneStrategy trappingAllForOne = new AllForOneStrategy(3, window, trapExit);
        trapExit.clear();
        assertEquals(SupervisorStrategy.Directive.Restart,
                trappingOneForOne.decider().apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Escalate,
                trappingOneForOne.decider().apply(new IllegalArgumentException("escalate")));
        assertEquals(SupervisorStrategy.Directive.Restart,
                trappingAllForOne.decider().apply(new IllegalStateException("restart")));
        assertEquals(SupervisorStrategy.Directive.Escalate,
                trappingAllForOne.decider().apply(new IllegalArgumentException("escalate")));

        OneForOneStrategy quietOneForOne = new OneForOneStrategy(false,
                error -> SupervisorStrategy.Directive.Resume);
        AllForOneStrategy quietAllForOne = new AllForOneStrategy(false,
                error -> SupervisorStrategy.Directive.Resume);
        assertEquals(-1, quietOneForOne.maxNrOfRetries());
        assertTrue(quietOneForOne.withinTimeRange().isNegative());
        assertFalse(quietOneForOne.loggingEnabled());
        assertEquals(-1, quietAllForOne.maxNrOfRetries());
        assertTrue(quietAllForOne.withinTimeRange().isNegative());
        assertFalse(quietAllForOne.loggingEnabled());

        OneForOneStrategy adjusted = quietOneForOne.withMaxNrOfRetries(4);
        assertEquals(4, adjusted.maxNrOfRetries());
        assertEquals(quietOneForOne.withinTimeRange(), adjusted.withinTimeRange());
        assertFalse(adjusted.loggingEnabled());
        assertEquals(SupervisorStrategy.Directive.Resume, adjusted.decider().apply(new IllegalStateException("resume")));

        assertSame(SupervisorStrategy.defaultStrategy, new DefaultSupervisorStrategy().create());
        assertSame(SupervisorStrategy.stoppingStrategy, new StoppingSupervisorStrategy().create());
        OneForOneStrategy stopping = (OneForOneStrategy) SupervisorStrategy.stoppingStrategy;
        assertEquals(-1, stopping.maxNrOfRetries());
        assertTrue(stopping.withinTimeRange().isNegative());
        assertEquals(SupervisorStrategy.Directive.Stop, stopping.decider().apply(new IllegalStateException("stop")));
        assertEquals(SupervisorStrategy.Directive.Escalate, stopping.decider().apply(new AssertionError("escalate")));
    }

    @Test
    void childRestartStatsEnforceRetryCountsAndWindowReset() throws Exception {
        system = ActorSystem.create("restart-stats");
        ActorRef child = system.actorOf(Props.create(SilentActor.class), "child");

        ChildRestartStats limited = new ChildRestartStats(child);
        assertTrue(limited.requestRestartPermission(2, null));
        assertTrue(limited.requestRestartPermission(2, null));
        assertFalse(limited.requestRestartPermission(2, null));
        assertFalse(new ChildRestartStats(child)
                .requestRestartPermission(0, null));
        assertTrue(new ChildRestartStats(child)
                .requestRestartPermission(-1, null));

        ChildRestartStats windowed = new ChildRestartStats(child);
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertTrue(windowed.requestRestartPermission(2, 60_000));
        assertFalse(windowed.requestRestartPermission(2, 60_000));

        ChildRestartStats expired = new ChildRestartStats(child, 2,
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

        parent.tell("fail-first", ActorRef.noSender);
        awaitInstance(children.get(0), 2);
        awaitInstance(children.get(1), 2);

        parent.tell("fail-first", ActorRef.noSender);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (children.stream().anyMatch(child -> !((InternalActorRef) child).isTerminated()) && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(children.stream().allMatch(child -> ((InternalActorRef) child).isTerminated()));
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
        parent.tell("fail-first", ActorRef.noSender);
        awaitInstance(children.get(0), 2);
        assertEquals(1, (int) ask(children.get(1), "instance"));

        parent.tell("fail-first", ActorRef.noSender);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!((InternalActorRef) children.get(0)).isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(((InternalActorRef) children.get(0)).isTerminated());
        assertFalse(((InternalActorRef) children.get(1)).isTerminated());
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
        children.get(0).tell("fail", ActorRef.noSender);
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
        children.get(0).tell("fail", ActorRef.noSender);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        java.util.List<ActorRef> restartedChildren = children;
        while (System.nanoTime() < deadline) {
            if (children.stream().allMatch(child -> ((InternalActorRef) child).isTerminated())) {
                restartedChildren = awaitChildren(parent);
                break;
            }
            Thread.yield();
        }
        assertFalse(((InternalActorRef) parent).isTerminated());
        assertTrue(children.stream().allMatch(child -> ((InternalActorRef) child).isTerminated()));
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
        children.get(0).tell("fail", ActorRef.noSender);
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
        children.get(0).tell("fail", ActorRef.noSender);
        Object event = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertInstanceOf(io.github.espen7.tapio.event.Warning.class, event);
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
        parent.tell("fail", ActorRef.noSender);

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
        system.stop(parent);

        assertTrue(childPostStop.await(WAIT_SECONDS, TimeUnit.SECONDS));
        assertFalse(((InternalActorRef) parent).isTerminated());
        allowChildStop.countDown();
        assertEquals(parent.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void actorPathWithoutAddressReturnsOnlyPathElements() throws Exception {
        assertEquals("/user/worker", ActorPath.fromString("tapio://system/user/worker").toStringWithoutAddress());
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
        assertEquals("Ask timed out on [" + actor + "] after [50 ms]. "
                        + "Message of type [java.lang.String]. A typical reason for `AskTimeoutException` is that "
                        + "the recipient actor didn't send a reply.", failure.getCause().getMessage());
    }

    @Test
    void askWithNonPositiveTimeoutReturnsFailedStageWithoutSending() throws Exception {
        system = ActorSystem.create("ask-invalid-timeout");
        BlockingQueue<Object> requests = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(EventSubscriberActor.class, requests), "probe");

        for (Duration timeout : new Duration[] {Duration.ZERO, Duration.ofMillis(-1)}) {
            CompletionStage<Object> result = Patterns.ask(actor, "request", timeout);
            assertTrue(result.toCompletableFuture().isCompletedExceptionally());
            CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
            assertInstanceOf(IllegalArgumentException.class, error.getCause());
        }
        actor.tell("barrier", ActorRef.noSender);
        assertEquals("barrier", requests.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void askToUnsupportedRecipientReturnsFailedStage() {
        CompletionStage<Object> result = Patterns.ask((ActorRef) null, 3.14, Duration.ofSeconds(1));

        assertTrue(result.toCompletableFuture().isCompletedExceptionally());
        CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals("Unsupported recipient type, question not sent to [null]. Message of type [java.lang.Double].",
                error.getCause().getMessage());
    }

    @Test
    void askToTerminatedActorFailsImmediatelyAndPublishesDeadLetter() throws Exception {
        system = ActorSystem.create("ask-terminated");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);
        ActorRef actor = system.actorOf(Props.create(EchoActor.class), "stopped");
        assertEquals("ready", ask(actor, "ready"));
        Mailbox mailbox = ((ActorCell) ((ActorRefWithCell) actor).underlying()).mailbox();
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        assertEquals("watching", ask(watcher, new Watch(actor)));
        system.stop(actor);
        assertEquals(actor.path().toString(), ask(watcher, "await-termination"));
        assertTrue(mailbox.isClosed());
        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ZERO);
        assertTrue(result.toCompletableFuture().isCompletedExceptionally());
        CompletionException error = assertThrows(CompletionException.class, () -> result.toCompletableFuture().join());
        assertInstanceOf(AskTimeoutException.class, error.getCause());
        assertEquals("Recipient [" + actor + "] had already been terminated. Message of type [java.lang.String].",
                error.getCause().getMessage());
        assertEquals(new DeadLetter("request", system.deadLetters(), actor), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void askUnwrapsClassicStatusReplies() throws Exception {
        system = ActorSystem.create("ask-status");
        ActorRef echo = system.actorOf(Props.create(EchoActor.class), "echo");

        assertEquals("accepted", ask(echo, new Status.Success("accepted")));
        IllegalStateException cause = new IllegalStateException("rejected");
        ExecutionException failure = assertThrows(ExecutionException.class, () -> ask(echo, new Status.Failure(cause)));
        assertSame(cause, failure.getCause());
    }

    @Test
    void askReplyRefRegistersNotifiesWatchersAndSendsDuplicateReplyToDeadLetters() throws Exception {
        system = ActorSystem.create("ask-ref");
        BlockingQueue<ActorRef> replies = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(AskSenderProbe.class, replies), "probe");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);
        ActorRefProvider provider = ((ActorSystemImpl) system).provider();
        VirtualPathContainer temp = (VirtualPathContainer) provider.tempContainer();

        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ofSeconds(WAIT_SECONDS));
        ActorRef replyTo = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(replyTo);
        ActorPath path = replyTo.path();
        assertTrue(path.toString().startsWith("tapio://ask-ref/temp/probe$"));
        assertSame(replyTo, temp.getChild(path.name()));
        assertSame(replyTo, provider.resolveActorRef(path.toString()));
        assertEquals("watching", ask(watcher, new Watch(replyTo)));

        system.actorSelection(path).tell("response", actor);
        assertEquals("response", result.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertEquals(path.toString(), ask(watcher, "await-termination"));
        assertTrue(((InternalActorRef) replyTo).isTerminated());
        assertSame(path, replyTo.path());
        assertNull(temp.getChild(path.name()));
        assertInstanceOf(EmptyLocalActorRef.class, provider.resolveActorRef(path));

        replyTo.tell("duplicate", actor);
        assertEquals(new DeadLetter("duplicate", actor, replyTo), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void timedOutReplyRefStaysUnregisteredWhenPathIsFirstReadAfterStop() throws Exception {
        system = ActorSystem.create("ask-late-reply");
        BlockingQueue<ActorRef> replies = new LinkedBlockingQueue<>();
        ActorRef actor = system.actorOf(Props.create(AskSenderProbe.class, replies), "probe");
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(EventSubscriberActor.class, events), "subscriber");
        system.eventStream().subscribe(subscriber, DeadLetter.class);

        CompletionStage<Object> result = Patterns.ask(actor, "request", Duration.ofMillis(50));
        ActorRef replyTo = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(replyTo);
        ExecutionException error = assertThrows(ExecutionException.class,
                () -> result.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS));
        assertInstanceOf(AskTimeoutException.class, error.getCause());
        // 定时器顺序执行，后一项任务运行时 ask 的清理回调已经返回。
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        system.scheduler().scheduleOnce(Duration.ofNanos(1), () -> barrier.complete(null));
        barrier.get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertTrue(((InternalActorRef) replyTo).isTerminated());
        ActorPath path = replyTo.path();
        VirtualPathContainer temp = (VirtualPathContainer) ((ActorSystemImpl) system).provider().tempContainer();
        assertNull(temp.getChild(path.name()));
        replyTo.tell("late", actor);
        assertEquals(new DeadLetter("late", actor, replyTo), events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void schedulerCloseRunsOnlyMarkedPendingTasks() throws Exception {
        system = ActorSystem.create("scheduler-close");
        AtomicInteger calls = new AtomicInteger();
        Scheduler scheduler = system.scheduler();
        Duration delay = Duration.ofHours(1);
        ActorRef quietActor = system.actorOf(Props.create(SilentActor.class), "silent");
        CompletionStage<Object> pendingAsk = Patterns.ask(quietActor, "pending", delay);
        scheduler.scheduleOnce(delay, () -> calls.addAndGet(100));
        scheduler.scheduleOnce(delay, (Scheduler.TaskRunOnClose) calls::incrementAndGet);
        Cancellable cancelled = scheduler.scheduleOnce(delay, (Scheduler.TaskRunOnClose) () -> calls.addAndGet(10));
        assertTrue(cancelled.cancel());
        assertTrue(cancelled.isCancelled());
        assertThrows(IllegalArgumentException.class, () -> scheduler.scheduleOnce(Duration.ofDays(3650), () -> {}));

        system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, calls.get());
        assertFalse(pendingAsk.toCompletableFuture().isDone());
        assertThrows(IllegalStateException.class, () -> scheduler.scheduleOnce(delay, () -> {}));
    }

    public static final class AskSenderProbe extends AbstractActor {
        private final BlockingQueue<ActorRef> replies;

        public AskSenderProbe(BlockingQueue<ActorRef> replies) {
            this.replies = replies;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> replies.add(getSender())).build();
        }
    }

    @Test
    void watcherReceivesTerminatedAfterWatchedActorStops() throws Exception {
        system = ActorSystem.create("death-watch");
        ActorRef watched = system.actorOf(Props.create(SilentActor.class), "watched");
        ActorRef watcher = system.actorOf(Props.create(WatcherActor.class), "watcher");

        assertEquals("watching", ask(watcher, new Watch(watched)));
        system.stop(watched);

        assertEquals(watched.path().toString(), ask(watcher, "await-termination"));
    }

    @Test
    void childFailureRestartsActorInstance() throws Exception {
        system = ActorSystem.create("restart");
        AtomicInteger instances = new AtomicInteger();
        ActorRef child = system.actorOf(Props.create(RestartingActor.class,
                () -> new RestartingActor(instances.incrementAndGet())), "restartable");

        assertEquals(1, ask(child, "instance"));
        child.tell("fail", ActorRef.noSender);
        assertEquals(2, ask(child, "instance"));
        assertEquals(2, instances.get());
    }

    @Test
    void configuredGuardianSupervisorStrategyStopsTopLevelActor() throws Exception {
        Config config = ConfigFactory.parseString("""
                tapio.actor.guardian-supervisor-strategy =
                  "io.github.espen7.tapio.actor.ActorRuntimeTest$StoppingSupervisorStrategyConfigurator"
                """);
        system = ActorSystem.create("guardian-strategy", config);
        assertEquals(StoppingSupervisorStrategyConfigurator.class.getName(),
                system.settings().SupervisorStrategyClass);
        AtomicInteger instances = new AtomicInteger();
        ActorRef actor = system.actorOf(Props.create(RestartingActor.class,
                () -> new RestartingActor(instances.incrementAndGet())), "actor");

        assertEquals(1, ask(actor, "instance"));
        actor.tell("fail", ActorRef.noSender);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!((InternalActorRef) actor).isTerminated() && System.nanoTime() < deadline) Thread.yield();
        assertTrue(((InternalActorRef) actor).isTerminated());
        assertEquals(1, instances.get());
    }

    @Test
    void systemGuardianWaitsForTheRegisteredTerminationHook() throws Exception {
        system = ActorSystem.create("guardian-termination-hook");
        ActorSystemImpl impl = (ActorSystemImpl) system;
        ActorRef guardian = impl.provider().systemGuardian();
        CountDownLatch registered = new CountDownLatch(1);
        BlockingQueue<ActorRef> notifications = new LinkedBlockingQueue<>();
        ActorRef hook = impl.systemActorOf(Props.create(TerminationHookActor.class, guardian, registered,
                notifications), "hook");

        assertTrue(registered.await(WAIT_SECONDS, TimeUnit.SECONDS));
        var terminated = system.terminate().toCompletableFuture();
        try {
            assertSame(guardian, notifications.poll(WAIT_SECONDS, TimeUnit.SECONDS));
            assertFalse(terminated.isDone());
        } finally {
            guardian.tell(SystemGuardian.TerminationHookDone.INSTANCE, hook);
        }
        assertSame(impl.provider().rootGuardian(), terminated.get(WAIT_SECONDS, TimeUnit.SECONDS).actor());
    }

    public static final class TerminationHookActor extends AbstractActor {
        private final ActorRef guardian;
        private final CountDownLatch registered;
        private final BlockingQueue<ActorRef> notifications;

        public TerminationHookActor(ActorRef guardian, CountDownLatch registered,
                                    BlockingQueue<ActorRef> notifications) {
            this.guardian = guardian;
            this.registered = registered;
            this.notifications = notifications;
        }

        @Override
        public void preStart() {
            guardian.tell(SystemGuardian.RegisterTerminationHook.INSTANCE, getSelf());
            guardian.tell(new Identify("termination-hook"), getSelf());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(ActorIdentity.class, identity -> registered.countDown())
                    .match(SystemGuardian.TerminationHook.class, hook -> notifications.add(getSender()))
                    .build();
        }
    }

    @Test
    void terminateCompletesSystemTerminationStage() throws Exception {
        system = ActorSystem.create("terminate");
        ActorRefProvider provider = ((ActorSystemImpl) system).provider();
        BlockingQueue<String> stopped = new LinkedBlockingQueue<>();
        Props props = Props.create(StopProbe.class, stopped);
        ActorRef actor = system.actorOf(props, "actor");
        ActorRef service = ((ExtendedActorSystem) system).systemActorOf(props, "service");
        assertEquals("ready", ask(actor, "ready"));
        assertEquals("ready", ask(service, "ready"));

        Terminated terminated = system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);

        assertEquals("/user/actor", stopped.poll());
        assertEquals("/system/service", stopped.poll());
        assertTrue(((InternalActorRef) actor).isTerminated());
        assertTrue(((InternalActorRef) service).isTerminated());
        assertTrue(provider.guardian().isTerminated());
        assertTrue(provider.systemGuardian().isTerminated());
        assertSame(provider.rootGuardian(), terminated.actor());
        assertTrue(((InternalActorRef) terminated.actor()).isTerminated());
        assertTrue(terminated.existenceConfirmed());
        assertTrue(terminated.addressTerminated());
        assertTrue(system.getWhenTerminated().toCompletableFuture().isDone());
    }

    public static class StopProbe extends EchoActor {
        private final BlockingQueue<String> stopped;

        public StopProbe(BlockingQueue<String> stopped) {
            this.stopped = stopped;
        }

        @Override
        public void postStop() {
            stopped.add(getSelf().path().toStringWithoutAddress());
        }
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

    public static class FactoryProbe extends AbstractActor {
        private ActorRef child;

        @Override
        public Receive createReceive() {
            ActorRefFactory factory = getContext();
            return receiveBuilder()
                    .match(String.class, message -> {
                        if (message.equals("create-child")) {
                            child = factory.actorOf(Props.create(EchoActor.class));
                            getSender().tell(child, getSelf());
                        } else if (message.equals("stop-child")) {
                            factory.stop(child);
                            getSender().tell("stopping", getSelf());
                        } else if (message.equals("select-child")) {
                            factory.actorSelection(child.path().name()).forward("relative", getContext());
                        }
                    })
                    .build();
        }
    }

    public static class LoggingActor extends AbstractLoggingActor {
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
        public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
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

    public static final class StoppingSupervisorStrategyConfigurator implements SupervisorStrategyConfigurator {
        @Override
        public SupervisorStrategy create() {
            return SupervisorStrategy.stoppingStrategy;
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
                            getSender().tell(StreamSupport.stream(getContext().getChildren().spliterator(), false).toList(), getSelf());
                        } else if (message.equals("fail-first")) {
                            StreamSupport.stream(getContext().getChildren().spliterator(), false)
                                    .filter(child -> child.path().name().equals("first"))
                                    .findFirst().orElseThrow().tell("fail", getSelf());
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
