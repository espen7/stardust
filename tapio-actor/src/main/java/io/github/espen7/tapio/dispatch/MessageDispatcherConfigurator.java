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

/** 根据有效配置创建 MessageDispatcher。 */
public abstract class MessageDispatcherConfigurator {
    private final Config config;
    private final DispatcherPrerequisites prerequisites;

    /**
     * 保存配置和构造 Dispatcher 所需的运行时组件。
     *
     * @param _config 有效配置
     * @param prerequisites 运行时组件
     */
    protected MessageDispatcherConfigurator(Config _config, DispatcherPrerequisites prerequisites) {
        this.config = _config;
        this.prerequisites = prerequisites;
    }

    /**
     * 返回有效配置。
     *
     * @return 配置
     */
    public Config config() {
        return config;
    }

    /**
     * 返回运行时组件。
     *
     * @return prerequisites
     */
    public DispatcherPrerequisites prerequisites() {
        return prerequisites;
    }

    /**
     * 创建或返回 MessageDispatcher。
     *
     * @return MessageDispatcher
     */
    public abstract MessageDispatcher dispatcher();

    /**
     * 按配置创建 executor configurator。
     *
     * @return executor configurator
     */
    public ExecutorServiceConfigurator configureExecutor() {
        String executor = config.getString("executor");
        if ("default-executor".equals(executor)) {
            Config defaultConfig = config.getConfig("default-executor");
            return new DefaultExecutorServiceConfigurator(defaultConfig, prerequisites,
                    configurator(defaultConfig.getString("fallback")));
        }
        return configurator(executor);
    }

    private ExecutorServiceConfigurator configurator(String executor) {
        if (executor == null) return new ForkJoinExecutorConfigurator(
                config.getConfig("fork-join-executor"), prerequisites);
        return switch (executor) {
            case "", "fork-join-executor" -> new ForkJoinExecutorConfigurator(
                    config.getConfig("fork-join-executor"), prerequisites);
            case "thread-pool-executor" -> new ThreadPoolExecutorConfigurator(
                    config.getConfig("thread-pool-executor"), prerequisites);
            case "virtual-thread-executor" -> new VirtualThreadExecutorConfigurator(
                    config.getConfig("virtual-thread-executor"), prerequisites);
            default -> {
                Config execConfig = config.hasPath(executor) ? config.getConfig(executor) : com.typesafe.config.ConfigFactory.empty();
                try {
                    yield prerequisites.dynamicAccess().createInstanceFor(
                            executor,
                            ExecutorServiceConfigurator.class,
                            java.util.List.<java.util.Map.Entry<Class<?>, Object>>of(
                                    java.util.Map.entry(Config.class, execConfig),
                                    java.util.Map.entry(DispatcherPrerequisites.class, prerequisites)));
                } catch (Throwable failure) {
                    throw new ConfigurationException("Cannot instantiate ExecutorServiceConfigurator type [" + executor
                            + "], defined in [" + config.getString("id")
                            + "], make sure it has constructor with [com.typesafe.config.Config] and ["
                            + DispatcherPrerequisites.class.getName() + "] parameters", failure);
                }
            }
        };
    }
}
