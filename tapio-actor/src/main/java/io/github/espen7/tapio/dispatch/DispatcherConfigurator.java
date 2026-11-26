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
import io.github.espen7.tapio.ConfigurationException;
import io.github.espen7.tapio.actor.ActorSystem;

import java.util.concurrent.ThreadFactory;

/** 读取有效配置，并为普通 Dispatcher 保留唯一实例。 */
final class DispatcherConfigurator extends MessageDispatcherConfigurator {
    private final Dispatcher instance;

    DispatcherConfigurator(Config config, ActorSystem system) {
        super(config);
        instance = new Dispatcher(config.getInt("throughput"), config.getDuration("throughput-deadline-time"),
                system, system.scheduler(), config.getDuration("shutdown-timeout"),
                createExecutorServiceFactory(config, system.threadFactory()));
    }

    static ExecutorServiceFactory createExecutorServiceFactory(Config config, ThreadFactory threadFactory) {
        return executorConfigurator(config).createExecutorServiceFactory(config.getString("id"), threadFactory);
    }

    static ExecutorServiceConfigurator executorConfigurator(Config config) {
        return configurator(config.getString("executor"), config);
    }

    private static ExecutorServiceConfigurator configurator(String executor, Config config) {
        return switch (executor) {
            case "", "fork-join-executor" -> new ForkJoinExecutorConfigurator(config.getConfig("fork-join-executor"));
            case "thread-pool-executor" -> new ThreadPoolExecutorConfigurator(config.getConfig("thread-pool-executor"));
            case "default-executor" -> new DefaultExecutorServiceConfigurator(
                    configurator(config.getConfig("default-executor").getString("fallback"), config));
            default -> throw new ConfigurationException("Cannot instantiate ExecutorServiceConfigurator type [" + executor
                    + "], defined in [" + config.getString("id")
                    + "]. Supported built-ins are default-executor, fork-join-executor and thread-pool-executor.");
        };
    }

    Dispatcher dispatcher() {
        return instance;
    }
}
