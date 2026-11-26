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
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.actor.ActorPath;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按配置 ID 查找或创建 MessageDispatcher。普通 Dispatcher 及其别名共享同一个实例，
 * PinnedDispatcher 及其别名则在每次查找时创建新实例。
 */
public class Dispatchers {
    /** 默认 Dispatcher 的 ID，也是其完整配置路径。 */
    public static final String DefaultDispatcherId = ActorPath.Scheme + ".actor.default-dispatcher";
    /** INTERNAL API：Pekko 内部 Dispatcher 的配置路径。 */
    @InternalApi
    public static final String InternalDispatcherId = ActorPath.Scheme + ".actor.internal-dispatcher";
    private static final int MaxDispatcherAliasDepth = 20;

    private final ActorSystem.Settings settings;
    private final DispatcherPrerequisites prerequisites;
    private final LoggingAdapter logger;
    private final Config defaultDispatcherConfig;
    private final ConcurrentHashMap<String, MessageDispatcherConfigurator> dispatcherConfigurators =
            new ConcurrentHashMap<>();
    private final MessageDispatcher internalDispatcher;

    /**
     * 从 ActorSystem 当前组件创建 Dispatcher 注册表。
     *
     * @param settings 所属系统的有效配置
     * @param system 接收 dead-letter 事件并提供 Scheduler 的系统
     */
    public Dispatchers(ActorSystem.Settings settings, ActorSystem system) {
        this(settings, new DefaultDispatcherPrerequisites(system.threadFactory(), system.eventStream(),
                system.scheduler(), settings, system.mailboxes(), Optional.empty()), system.log());
    }

    /**
     * 从显式运行时组件创建 Dispatcher 注册表。
     *
     * @param settings 系统设置
     * @param prerequisites Dispatcher 运行时组件
     * @param logger 注册表日志适配器
     */
    public Dispatchers(ActorSystem.Settings settings, DispatcherPrerequisites prerequisites, LoggingAdapter logger) {
        this.settings = settings;
        this.prerequisites = prerequisites;
        this.logger = logger;
        this.defaultDispatcherConfig = idConfig(DefaultDispatcherId)
                .withFallback(settings.config().getConfig(DefaultDispatcherId));
        this.internalDispatcher = lookup(InternalDispatcherId);
    }

    /**
     * 返回供 Pekko 内部 Actor 和任务使用的 Dispatcher。
     *
     * @return 此 ActorSystem 唯一的 internal Dispatcher
     */
    @InternalApi
    public MessageDispatcher internalDispatcher() {
        return internalDispatcher;
    }

    /**
     * 返回此系统唯一的默认 Dispatcher。
     *
     * @return 默认 Dispatcher
     * @throws ConfigurationException 当默认 Dispatcher 的类型不受支持时
     */
    public MessageDispatcher defaultGlobalDispatcher() {
        return lookup(DefaultDispatcherId);
    }

    /**
     * 查找 Dispatcher。配置值可以是配置段，也可以是指向另一个 ID 的字符串。
     * 注册表缓存 configurator；普通 Dispatcher 复用实例，PinnedDispatcher 每次查找创建新实例。
     *
     * @param id 配置路径或已配置的别名
     * @return 此 ID 对应的 Dispatcher
     * @throws ConfigurationException 当配置缺失、别名超过深度限制或类型不受支持时
     * @throws com.typesafe.config.ConfigException 当 Dispatcher 参数无效时
     */
    public MessageDispatcher lookup(String id) {
        return lookupConfigurator(id, 0).dispatcher();
    }

    /**
     * 返回指定 Dispatcher 的有效配置，包含合并后的 mailbox 相关键。
     *
     * @param id 配置路径或别名
     * @return 与已创建 Dispatcher 对应的有效配置
     * @throws ConfigurationException 当配置缺失、别名超过深度限制或类型不受支持时
     * @throws com.typesafe.config.ConfigException 当 Dispatcher 参数无效时
     */
    public Config config(String id) {
        return lookupConfigurator(id, 0).config();
    }

    /**
     * 检查指定 ID 是否已缓存或存在配置项。
     *
     * @param id 配置路径或别名
     * @return 找到 ID 时为 {@code true}。配置内容在 {@link #lookup(String)} 时校验
     */
    public boolean hasDispatcher(String id) {
        return dispatcherConfigurators.containsKey(id) || settings.config().hasPath(id);
    }

    private MessageDispatcherConfigurator lookupConfigurator(String id, int depth) {
        if (depth > MaxDispatcherAliasDepth) {
            throw new ConfigurationException(
                    "Didn't find a concrete dispatcher config after following " + MaxDispatcherAliasDepth
                            + ", is there a loop in your config? last looked for id was " + id);
        }
        MessageDispatcherConfigurator existing = dispatcherConfigurators.get(id);
        if (existing != null) return existing;

        Config config = settings.config();
        if (!config.hasPath(id)) throw new ConfigurationException("Dispatcher [" + id + "] not configured");
        var valueAtPath = config.getValue(id);
        MessageDispatcherConfigurator newConfigurator = switch (valueAtPath.valueType()) {
            case STRING -> {
                String actualId = (String) valueAtPath.unwrapped();
                logger.debug("Dispatcher id [{}] is an alias, actual dispatcher will be [{}]", id, actualId);
                yield lookupConfigurator(actualId, depth + 1);
            }
            case OBJECT -> configuratorFrom(config(id, config.getConfig(id)));
            default -> throw new ConfigurationException(
                    "Expected either a dispatcher config or an alias at [" + id + "] but found ["
                            + valueAtPath.valueType() + "]");
        };

        existing = dispatcherConfigurators.putIfAbsent(id, newConfigurator);
        return existing == null ? newConfigurator : existing;
    }

    private Config config(String id, Config appConfig) {
        String simpleName = id.substring(id.lastIndexOf('.') + 1);
        return idConfig(id)
                .withFallback(appConfig)
                .withFallback(ConfigFactory.parseMap(Map.of("name", simpleName)))
                .withFallback(defaultDispatcherConfig);
    }

    private static Config idConfig(String id) {
        return ConfigFactory.parseMap(Map.of("id", id));
    }

    private MessageDispatcherConfigurator configuratorFrom(Config config) {
        String type = config.getString("type");
        if ("Dispatcher".equals(type)) return new DispatcherConfigurator(config, prerequisites);
        if ("PinnedDispatcher".equals(type)) return new PinnedDispatcherConfigurator(config, prerequisites);
        throw new ConfigurationException("Cannot instantiate MessageDispatcherConfigurator type [" + type
                + "], defined in [" + config.getString("id") + "]");
    }
}
