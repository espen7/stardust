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

import com.typesafe.config.ConfigFactory;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Scheduler;

import java.time.Duration;
import java.util.Optional;

public final class DispatcherTestSupport {
    private DispatcherTestSupport() {
    }

    public static DispatcherPrerequisites prerequisites(ActorSystem system, Scheduler scheduler) {
        return new DefaultDispatcherPrerequisites(system.threadFactory(), system.eventStream(), scheduler,
                system.settings(), system.mailboxes(), Optional.empty());
    }

    public static MessageDispatcherConfigurator configurator(ActorSystem system, Scheduler scheduler) {
        return new MessageDispatcherConfigurator(ConfigFactory.empty(), prerequisites(system, scheduler)) {
            @Override
            public MessageDispatcher dispatcher() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Dispatcher dispatcher(ActorSystem system, Scheduler scheduler, ExecutorServiceFactory factory) {
        return new Dispatcher(configurator(system, scheduler), "test-dispatcher", 5, Duration.ofMillis(100),
                (id, threadFactory) -> factory, Duration.ofHours(1));
    }

    public static Dispatcher configuredDispatcher(ActorSystem system) {
        DispatcherPrerequisites prerequisites = prerequisites(system, system.scheduler());
        return (Dispatcher) new DispatcherConfigurator(
                system.dispatchers().config(Dispatchers.DefaultDispatcherId), prerequisites).dispatcher();
    }
}
