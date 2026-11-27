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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.setup.ActorSystemSetup;
import io.github.espen7.tapio.dispatch.Dispatchers;
import io.github.espen7.tapio.dispatch.MessageDispatcher;
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClassicBoundaryAlignmentTest {

    abstract static class AbstractTestActor extends AbstractActor {
    }

    static final class CustomGuardianActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, "dispatcher"::equals,
                            msg -> sender().tell(getContext().dispatcher().id(), getSelf()))
                    .matchAny(msg -> sender().tell("guardian:" + msg, getSelf()))
                    .build();
        }
    }

    @Test
    void propsValidateRejectsAbstractActorImmediately() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> Props.create(AbstractTestActor.class));
        assertTrue(error.getMessage().contains("must not be abstract"));
    }

    @Test
    void customGuardianPropsRunsOnUserDispatcherAndRejectsExternalActorOf() throws Exception {
        var config = ConfigFactory.parseString("""
                custom-disp {
                  type = Dispatcher
                }
                """);
        Props guardianProps = Props.create(CustomGuardianActor.class).withDispatcher("custom-disp");
        try (ActorSystem system = new ActorSystemImpl(
                "custom-guardian",
                config,
                ActorSystemSetup.empty(),
                Optional.of(guardianProps)).start()) {
            assertThrows(UnsupportedOperationException.class,
                    () -> system.actorOf(Props.empty));
            assertThrows(UnsupportedOperationException.class,
                    () -> system.actorOf(Props.empty, "child"));

            ActorRef userGuardian = ((ActorSystemImpl) system).provider().guardian();
            Object reply = Patterns.ask(userGuardian, "dispatcher", Duration.ofSeconds(3))
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS);
            assertEquals("custom-disp", reply);

            Object echo = Patterns.ask(userGuardian, "hello", Duration.ofSeconds(3))
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS);
            assertEquals("guardian:hello", echo);
        }
    }

    @Test
    void customGuardianWithDefaultSameAsParentUsesDefaultDispatcher() throws Exception {
        Props guardianProps = Props.create(CustomGuardianActor.class)
                .withDispatcher(Deploy.DispatcherSameAsParent);
        try (ActorSystem system = new ActorSystemImpl(
                "custom-guardian-default",
                null,
                ActorSystemSetup.empty(),
                Optional.of(guardianProps)).start()) {
            ActorRef userGuardian = ((ActorSystemImpl) system).provider().guardian();
            Object reply = Patterns.ask(userGuardian, "dispatcher", Duration.ofSeconds(3))
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS);
            assertEquals(Dispatchers.DefaultDispatcherId, reply);
        }
    }

    @Test
    void schedulerAndMessageDispatcherDoNotImplementAutoCloseable() {
        assertFalse(AutoCloseable.class.isAssignableFrom(Scheduler.class));
        assertFalse(AutoCloseable.class.isAssignableFrom(MessageDispatcher.class));
        assertTrue(Closeable.class.isAssignableFrom(LightArrayRevolverScheduler.class));
    }
}
