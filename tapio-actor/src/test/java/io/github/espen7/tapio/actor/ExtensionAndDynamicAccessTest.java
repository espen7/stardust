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
import io.github.espen7.tapio.pattern.Patterns;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExtensionAndDynamicAccessTest {
    static final class SampleExt implements Extension {
        final String systemName;

        SampleExt(ExtendedActorSystem system) {
            this.systemName = system.settings().name();
        }
    }

    static final class SampleExtId extends AbstractExtensionId<SampleExt> implements ExtensionIdProvider {
        static final SampleExtId INSTANCE = new SampleExtId();
        static final AtomicInteger CREATED = new AtomicInteger();

        @Override
        public ExtensionId<? extends Extension> lookup() {
            return INSTANCE;
        }

        @Override
        public SampleExt createExtension(ExtendedActorSystem system) {
            CREATED.incrementAndGet();
            return new SampleExt(system);
        }
    }

    static final class ConfiguredExtProvider implements ExtensionIdProvider {
        @Override
        public ExtensionId<? extends Extension> lookup() {
            return SampleExtId.INSTANCE;
        }
    }

    static final class NullExtId extends AbstractExtensionId<SampleExt> {
        @Override
        public SampleExt createExtension(ExtendedActorSystem system) {
            return null;
        }
    }

    static final class VirtualThreadProbeActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(String.class, msg -> getSender().tell(Thread.currentThread().isVirtual(), getSelf()))
                    .build();
        }
    }

    @Test
    void extensionRegistrationIsSingletonAndValidatesUnregistered() throws Exception {
        SampleExtId.CREATED.set(0);
        try (ActorSystem system = ActorSystem.create("ext-test")) {
            assertFalse(system.hasExtension(SampleExtId.INSTANCE));
            assertThrows(IllegalArgumentException.class, () -> system.extension(SampleExtId.INSTANCE));

            SampleExt first = SampleExtId.INSTANCE.get(system);
            SampleExt second = system.extension(SampleExtId.INSTANCE);
            assertTrue(system.hasExtension(SampleExtId.INSTANCE));
            assertSame(first, second);
            assertEquals("ext-test", first.systemName);
            assertEquals(1, SampleExtId.CREATED.get());

            assertThrows(IllegalStateException.class, () -> system.registerExtension(new NullExtId()));
        }
    }

    @Test
    void concurrentExtensionRegistrationInitializesOnce() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExtensionId<SampleExt> slowExt = new AbstractExtensionId<>() {
            @Override
            public SampleExt createExtension(ExtendedActorSystem system) {
                started.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                return new SampleExt(system);
            }
        };

        try (ActorSystem system = ActorSystem.create("ext-concurrent")) {
            CompletableFuture<SampleExt> f1 = CompletableFuture.supplyAsync(() -> slowExt.get(system));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            CompletableFuture<SampleExt> f2 = CompletableFuture.supplyAsync(() -> slowExt.get(system));
            release.countDown();
            assertSame(f1.get(2, TimeUnit.SECONDS), f2.get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void extensionsAreLoadedFromConfigAndLibraryExtensionsFailFast() throws Exception {
        Config validCfg = ConfigFactory.parseMap(Map.of(
                "tapio.extensions", List.of(ConfiguredExtProvider.class.getName(), "no.such.OptionalExtension")));
        try (ActorSystem system = ActorSystem.create("ext-config", validCfg)) {
            assertTrue(system.hasExtension(SampleExtId.INSTANCE));
        }

        Config badLibraryCfg = ConfigFactory.parseMap(Map.of(
                "tapio.library-extensions", List.of("no.such.RequiredExtension")));
        assertThrows(RuntimeException.class, () -> ActorSystem.create("ext-lib-fail", badLibraryCfg));
    }

    @Test
    void dynamicAccessResolvesAndInstantiatesClasses() throws Throwable {
        try (ActorSystem system = ActorSystem.create("dyn-test")) {
            ExtendedActorSystem ext = (ExtendedActorSystem) system;
            assertTrue(ext.dynamicAccess().classIsOnClasspath(DefaultSupervisorStrategy.class.getName()));
            assertFalse(ext.dynamicAccess().classIsOnClasspath("no.such.MissingClass"));
            SupervisorStrategyConfigurator configurator = ext.dynamicAccess().createInstanceFor(
                    DefaultSupervisorStrategy.class.getName(), SupervisorStrategyConfigurator.class, List.of());
            assertSame(SupervisorStrategy.defaultStrategy, configurator.create());
        }
    }

    @Test
    void actorCanRunOnVirtualThreadDispatcher() throws Exception {
        Config cfg = ConfigFactory.parseMap(Map.of(
                "tapio.actor.vt-dispatcher.type", "Dispatcher",
                "tapio.actor.vt-dispatcher.executor", "virtual-thread-executor"));
        try (ActorSystem system = ActorSystem.create("vt-actor-system", cfg)) {
            ActorRef actor = system.actorOf(
                    Props.create(VirtualThreadProbeActor.class).withDispatcher("tapio.actor.vt-dispatcher"), "probe");
            Object result = Patterns.ask(actor, "ping", Duration.ofSeconds(2)).toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(Boolean.TRUE, result);
        }
    }
}
