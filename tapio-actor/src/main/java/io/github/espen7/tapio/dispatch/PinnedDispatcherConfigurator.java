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
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.event.Warning;

/** 为 PinnedDispatcher 保留配置，并在每次查找时创建新实例。 */
final class PinnedDispatcherConfigurator extends MessageDispatcherConfigurator {
    private final ActorSystem system;
    private final ThreadPoolConfig threadPoolConfig;

    PinnedDispatcherConfigurator(Config config, ActorSystem system) {
        super(config);
        this.system = system;
        ExecutorServiceConfigurator executor = DispatcherConfigurator.executorConfigurator(config);
        if (executor instanceof ThreadPoolExecutorConfigurator threadPool) {
            this.threadPoolConfig = threadPool.threadPoolConfig();
        } else {
            system.eventStream().publish(new Warning("PinnedDispatcherConfigurator", getClass(),
                    "PinnedDispatcher [" + config.getString("id")
                            + "] not configured to use ThreadPoolExecutor, falling back to default config."));
            this.threadPoolConfig = ThreadPoolConfig.defaults();
        }
    }

    @Override
    Dispatcher dispatcher() {
        return new PinnedDispatcher(system, null, config().getString("id"),
                config().getDuration("shutdown-timeout"), threadPoolConfig);
    }
}
