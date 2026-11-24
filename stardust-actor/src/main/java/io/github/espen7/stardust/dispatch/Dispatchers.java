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
package io.github.espen7.stardust.dispatch;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.espen7.stardust.ConfigurationException;
import io.github.espen7.stardust.actor.ActorSystem;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按配置 ID 查找或创建 Dispatcher。普通 Dispatcher 及其别名共享同一个实例，
 * 生命周期由所属 ActorSystem 管理。
 */
public final class Dispatchers implements AutoCloseable {
    /** 默认 Dispatcher 的 ID，也是其完整配置路径。 */
    public static final String DefaultDispatcherId = "pekko.actor.default-dispatcher";
    /** Pekko 内部 Dispatcher 的配置路径。 */
    public static final String InternalDispatcherId = "pekko.actor.internal-dispatcher";
    private static final int MaxDispatcherAliasDepth = 20;

    private final ActorSystem.Settings settings;
    private final ActorSystem system;
    private final Config defaultDispatcherConfig;
    private final ConcurrentHashMap<String, DispatcherConfigurator> dispatcherConfigurators = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /**
     * 创建 ActorSystem 使用的 Dispatcher 注册表。
     *
     * @param settings 所属系统的有效配置
     * @param system 接收 dead-letter 事件并关闭此注册表的系统
     */
    public Dispatchers(ActorSystem.Settings settings, ActorSystem system) {
        this.settings = settings;
        this.system = system;
        this.defaultDispatcherConfig = idConfig(DefaultDispatcherId)
                .withFallback(settings.config().getConfig(DefaultDispatcherId));
    }

    /**
     * 返回此系统唯一的默认 Dispatcher。
     *
     * @return 默认 Dispatcher
     * @throws ConfigurationException 当默认 Dispatcher 的类型不受支持时
     * @throws IllegalStateException 当注册表已关闭时
     */
    public Dispatcher defaultGlobalDispatcher() {
        return lookup(DefaultDispatcherId);
    }

    /**
     * 查找普通 Dispatcher。配置值可以是配置段，也可以是指向另一个 ID 的字符串。
     * 首次成功查找后，同一个 ID 会重用已创建的 Dispatcher。
     *
     * @param id 配置路径或已配置的别名
     * @return 此 ID 对应的 Dispatcher
     * @throws ConfigurationException 当配置缺失、别名超过深度限制或类型不受支持时
     * @throws com.typesafe.config.ConfigException 当 Dispatcher 参数无效时
     * @throws IllegalStateException 当注册表已关闭时
     */
    public Dispatcher lookup(String id) {
        if (closed) throw new IllegalStateException("Dispatchers are closed");
        return lookupConfigurator(id, 0).dispatcher();
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

    private DispatcherConfigurator lookupConfigurator(String id, int depth) {
        if (depth > MaxDispatcherAliasDepth) {
            throw new ConfigurationException(
                    "Didn't find a concrete dispatcher config after following " + MaxDispatcherAliasDepth
                            + ", is there a loop in your config? last looked for id was " + id);
        }
        DispatcherConfigurator existing = dispatcherConfigurators.get(id);
        if (existing != null) return existing;

        Config config = settings.config();
        if (!config.hasPath(id)) throw new ConfigurationException("Dispatcher [" + id + "] not configured");
        var valueAtPath = config.getValue(id);
        DispatcherConfigurator newConfigurator = switch (valueAtPath.valueType()) {
            case STRING -> {
                String actualId = (String) valueAtPath.unwrapped();
                system.log().debug("Dispatcher id [{}] is an alias, actual dispatcher will be [{}]", id, actualId);
                yield lookupConfigurator(actualId, depth + 1);
            }
            case OBJECT -> configuratorFrom(config(id, config.getConfig(id)));
            default -> throw new ConfigurationException(
                    "Expected either a dispatcher config or an alias at [" + id + "] but found ["
                            + valueAtPath.valueType() + "]");
        };

        existing = dispatcherConfigurators.putIfAbsent(id, newConfigurator);
        if (existing != null && existing != newConfigurator) {
            newConfigurator.dispatcher().close();
        }
        DispatcherConfigurator selected = existing == null ? newConfigurator : existing;
        if (closed) {
            selected.dispatcher().close();
            throw new IllegalStateException("Dispatchers are closed");
        }
        return selected;
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

    private DispatcherConfigurator configuratorFrom(Config config) {
        String type = config.getString("type");
        if ("Dispatcher".equals(type)) return new DispatcherConfigurator(config, system);
        throw new ConfigurationException("Cannot instantiate MessageDispatcherConfigurator type [" + type
                + "], defined in [" + config.getString("id") + "]");
    }

    /** 停止所有已创建的 Dispatcher。通常由所属 ActorSystem 在终止时调用。 */
    @Override
    public void close() {
        closed = true;
        for (DispatcherConfigurator configurator : dispatcherConfigurators.values()) {
            configurator.dispatcher().close();
        }
    }
}
