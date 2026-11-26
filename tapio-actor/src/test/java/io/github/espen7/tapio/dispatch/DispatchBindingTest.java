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
import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.DeadLetter;
import io.github.espen7.tapio.actor.ActorCell;
import io.github.espen7.tapio.actor.ActorInitializationException;
import io.github.espen7.tapio.actor.ActorRefWithCell;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.event.Error;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 Props、部署配置和 mailbox 选择在 Actor 创建路径上的实际效果。
 */
class DispatchBindingTest {
    private static final long WAIT_SECONDS = 3;
    private static final String SMALL_MAILBOX = "tapio.small-mailbox";

    private ActorSystem system;

    @AfterEach
    void stopSystem() {
        if (system == null) return;
        try {
            system.terminate().toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception error) {
            throw new AssertionError("failed to stop actor system", error);
        } finally {
            system = null;
        }
    }

    @Test
    void defaultPropsResolveDefaultDispatcherAndMailbox() {
        system = ActorSystem.create("dispatch-default");

        assertEquals(Dispatchers.DefaultDispatcherId, Props.create(Echo.class).dispatcher());
        assertEquals(Mailboxes.DefaultMailboxId, Props.create(Echo.class).mailbox());
    }

    @Test
    void propsWithDispatcherAndMailboxAreImmutableAndUsedByActor() {
        system = ActorSystem.create("dispatch-props", customDispatcherConfig());

        Props base = Props.create(Echo.class);
        Props configured = base.withDispatcher("tapio.custom-dispatcher").withMailbox(SMALL_MAILBOX);

        assertEquals(Dispatchers.DefaultDispatcherId, base.dispatcher());
        assertEquals(Mailboxes.DefaultMailboxId, base.mailbox());
        assertEquals("tapio.custom-dispatcher", configured.dispatcher());
        assertEquals(SMALL_MAILBOX, configured.mailbox());

        var cell = underlying(system.actorOf(configured, "echo"));
        assertSame(system.dispatchers().lookup("tapio.custom-dispatcher"), cell.dispatcher());
        assertInstanceOf(BoundedMailbox.MessageQueue.class, cell.mailbox().messageQueue());
        assertEquals(7, cell.dispatcher().throughput());
    }

    @Test
    void deploymentConfigOverridesPropsDispatcherAndMailbox() {
        Config config = customDispatcherConfig().withFallback(ConfigFactory.parseString("""
                tapio.actor.deployment./overridden.dispatcher = "tapio.custom-dispatcher"
                tapio.actor.deployment./overridden.mailbox = "tapio.small-mailbox"
                """));
        system = ActorSystem.create("dispatch-deployment", config);

        var cell = underlying(system.actorOf(
                Props.create(Echo.class).withDispatcher("tapio.missing-dispatcher"), "overridden"));

        assertSame(system.dispatchers().lookup("tapio.custom-dispatcher"), cell.dispatcher());
        assertInstanceOf(BoundedMailbox.MessageQueue.class, cell.mailbox().messageQueue());
        assertEquals("tapio.custom-dispatcher", cell.props().dispatcher());
    }

    @Test
    void dispatcherSameAsParentUsesParentDispatcher() throws Exception {
        Config config = customDispatcherConfig().withFallback(ConfigFactory.parseString("""
                tapio.actor.deployment./parent/child.dispatcher = ".."
                """));
        system = ActorSystem.create("dispatch-same-as-parent", config);

        ActorRef parent = system.actorOf(
                Props.create(ParentActor.class).withDispatcher("tapio.custom-dispatcher"), "parent");
        ActorRef child = (ActorRef) ask(parent);

        var parentCell = underlying(parent);
        var childCell = underlying(child);
        assertSame(parentCell.dispatcher(), childCell.dispatcher());
        assertEquals("tapio.custom-dispatcher", childCell.props().dispatcher());
    }

    @Test
    void deploymentMailboxTakesPrecedenceOverDispatcherMailboxType() {
        Config config = ConfigFactory.parseString("""
                tapio.mailboxed-dispatcher {
                  type = "Dispatcher"
                  throughput = 5
                  throughput-deadline-time = 0ms
                  mailbox-requirement = ""
                  mailbox-type = "io.github.espen7.tapio.dispatch.BoundedMailbox"
                }
                tapio.actor.deployment./explicit-mailbox.mailbox = "unbounded"
                """);
        system = ActorSystem.create("dispatch-mailbox-order", config);

        var implicitCell = underlying(system.actorOf(
                Props.create(Echo.class).withDispatcher("tapio.mailboxed-dispatcher"), "implicit-mailbox"));
        var explicitCell = underlying(system.actorOf(
                Props.create(Echo.class).withDispatcher("tapio.mailboxed-dispatcher"), "explicit-mailbox"));

        assertInstanceOf(BoundedMailbox.MessageQueue.class, implicitCell.mailbox().messageQueue());
        assertInstanceOf(UnboundedMailbox.MessageQueue.class, explicitCell.mailbox().messageQueue());
    }

    @Test
    void fullBoundedMailboxPublishesDeadLetter() throws Exception {
        system = ActorSystem.create("dispatch-bounded", customDispatcherConfig());
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        system.eventStream().subscribe(
                system.actorOf(Props.create(Subscriber.class, events), "subscriber"), DeadLetter.class);

        ActorRef blocked = system.actorOf(
                Props.create(BlockingActor.class, events).withMailbox(SMALL_MAILBOX), "blocked");
        blocked.tell("occupy", system.deadLetters());
        assertEquals("occupied", events.poll(WAIT_SECONDS, TimeUnit.SECONDS));

        blocked.tell("one", system.deadLetters());
        blocked.tell("two", system.deadLetters());

        Object event = events.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        assertInstanceOf(DeadLetter.class, event);
        assertEquals("two", ((DeadLetter) event).message());
    }

    @Test
    void unknownDispatcherFailsBeforeCreatingActorAndNameCanBeReused() {
        system = ActorSystem.create("dispatch-invalid");

        assertThrows(ConfigurationException.class,
                () -> system.actorOf(Props.create(Echo.class).withDispatcher("not-configured"), "retry"));

        ActorRef actor = system.actorOf(Props.create(Echo.class), "retry");
        assertEquals("reply", await(actor));
    }

    @Test
    void actorRequirementRejectsIncompatibleMailbox() {
        system = ActorSystem.create("dispatch-requirement", customDispatcherConfig());

        assertThrows(ConfigurationException.class,
                () -> system.actorOf(Props.create(BoundedOnlyActor.class).withMailbox("unbounded"),
                        "bounded-only"));
    }

    @Test
    void lateMailboxRequirementFailureRetainsActorReference() throws Exception {
        system = ActorSystem.create("dispatch-requirement-runtime", ConfigFactory.parseString("""
                lying-mailbox.mailbox-type = "io.github.espen7.tapio.dispatch.DispatchBindingTest$LyingBoundedMailbox"
                """));
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ActorRef subscriber = system.actorOf(Props.create(Subscriber.class, events), "events");
        system.eventStream().subscribe(subscriber, Error.class);

        ActorRef actor = system.actorOf(Props.create(BoundedOnlyActor.class).withMailbox("lying-mailbox"),
                "bounded-only");
        Error error = assertInstanceOf(Error.class, events.poll(WAIT_SECONDS, TimeUnit.SECONDS));
        ActorInitializationException exception =
                assertInstanceOf(ActorInitializationException.class, error.cause());
        assertEquals(actor, exception.getActor());
        assertTrue(exception.getMessage().startsWith(actor.path() + ": "));
        assertTrue(exception.getMessage().contains("requires mailbox type ["));
        assertTrue(exception.getMessage().contains(UnboundedMailbox.MessageQueue.class.getName()));
    }

    @Test
    void actorRequirementSelectsMappedMailboxWithoutExplicitMailbox() {
        system = ActorSystem.create("dispatch-requirement-default");

        var cell = underlying(system.actorOf(Props.create(BoundedOnlyActor.class), "bounded-only"));

        assertInstanceOf(BoundedMailbox.MessageQueue.class, cell.mailbox().messageQueue());
    }

    @Test
    void deploymentWildcardMatchingUsesExactSingleAndDoublePrecedence() throws Exception {
        Config config = customDispatcherConfig().withFallback(ConfigFactory.parseString("""
                tapio.actor.deployment."/exact".dispatcher = "tapio.custom-dispatcher"
                tapio.actor.deployment."/*".mailbox = "tapio.small-mailbox"
                tapio.actor.deployment."/deep/**".mailbox = "tapio.small-mailbox"
                tapio.loggers = ["io.github.espen7.tapio.event.StandardOutLogger"]
                """));
        system = ActorSystem.create("dispatch-wildcards", config);

        var exact = underlying(system.actorOf(Props.create(Echo.class), "exact"));
        var single = underlying(system.actorOf(Props.create(Echo.class), "single"));
        ActorRef deep = system.actorOf(Props.create(ParentActor.class), "deep");
        var nested = underlying((ActorRef) ask(deep));

        assertEquals("tapio.custom-dispatcher", exact.props().dispatcher());
        assertInstanceOf(UnboundedMailbox.MessageQueue.class, exact.mailbox().messageQueue());
        assertInstanceOf(BoundedMailbox.MessageQueue.class, single.mailbox().messageQueue());
        assertInstanceOf(BoundedMailbox.MessageQueue.class, nested.mailbox().messageQueue());
    }

    @Test
    void deploymentRejectsInvalidDoubleWildcardSuffix() {
        Config config = ConfigFactory.parseString("""
                tapio.actor.deployment."/invalid**".dispatcher = "tapio.custom-dispatcher"
                """);

        assertThrows(IllegalArgumentException.class,
                () -> ActorSystem.create("dispatch-invalid-deployment", config));
    }

    private static Config customDispatcherConfig() {
        return ConfigFactory.parseString("""
                tapio.custom-dispatcher {
                  type = "Dispatcher"
                  throughput = 7
                  throughput-deadline-time = 0ms
                  mailbox-requirement = ""
                }
                tapio.small-mailbox {
                  mailbox-type = "io.github.espen7.tapio.dispatch.BoundedMailbox"
                  mailbox-capacity = 1
                  mailbox-push-timeout-time = 0ms
                }
                """);
    }

    private static ActorCell underlying(ActorRef actor) {
        ActorRefWithCell withCell = (ActorRefWithCell) actor;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        do {
            if (withCell.underlying() instanceof ActorCell cell) return cell;
            Thread.yield();
        } while (System.nanoTime() < deadline);
        return fail("ActorCell did not become available for " + actor);
    }

    private static Object ask(ActorRef actor) throws Exception {
        return Patterns.ask(actor, "child", Duration.ofSeconds(1))
                .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    private static Object await(ActorRef actor) {
        try {
            return Patterns.ask(actor, "reply", Duration.ofSeconds(1))
                    .toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception error) {
            throw new AssertionError(error);
        }
    }

    /** 原样回复任意消息。 */
    public static final class Echo extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(message -> getSender().tell(message, getSelf())).build();
        }
    }

    /** 创建 child 并把 child 引用回复给请求方。 */
    public static final class ParentActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if ("child".equals(message)) {
                            getSender().tell(getContext().actorOf(Props.create(Echo.class), "child"), getSelf());
                        }
                    })
                    .build();
        }
    }

    /** 收到 {@code occupy} 后阻塞 mailbox 线程，收到 {@code release} 时解除。 */
    public static final class BlockingActor extends AbstractActor {
        private final BlockingQueue<Object> events;
        private volatile boolean release;

        public BlockingActor(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, message -> {
                        if ("occupy".equals(message)) {
                            events.offer("occupied");
                            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                            while (!release && System.nanoTime() - deadline < 0) Thread.onSpinWait();
                        } else if ("release".equals(message)) {
                            release = true;
                        }
                    })
                    .build();
        }
    }

    /** 把收到的任意消息加入队列。 */
    public static final class Subscriber extends AbstractActor {
        private final BlockingQueue<Object> events;

        public Subscriber(BlockingQueue<Object> events) {
            this.events = events;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(events::add).build();
        }
    }

    public static final class LyingBoundedMailbox implements MailboxType,
            ProducesMessageQueue<BoundedMailbox.MessageQueue> {
        public LyingBoundedMailbox(ActorSystem.Settings settings, Config config) {
        }

        @Override
        public MessageQueue create(ActorRef owner, ActorSystem system) {
            return new UnboundedMailbox.MessageQueue();
        }
    }

    /** 声明必须有界队列的 Actor。 */
    public static final class BoundedOnlyActor extends AbstractActor
            implements RequiresMessageQueue<BoundedMessageQueueSemantics> {
        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(ignored -> {
            }).build();
        }
    }
}
