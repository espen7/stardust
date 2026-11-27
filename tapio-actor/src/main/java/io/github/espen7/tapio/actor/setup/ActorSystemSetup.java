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
package io.github.espen7.tapio.actor.setup;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 用于以编程方式向 {@link io.github.espen7.tapio.actor.ActorSystem} 传递可选配置对象的不可变容器。
 */
public final class ActorSystemSetup {
    private static final ActorSystemSetup EMPTY = new ActorSystemSetup(Map.of());

    private final Map<Class<?>, Object> setups;

    private ActorSystemSetup(Map<Class<?>, Object> setups) {
        this.setups = Map.copyOf(setups);
    }

    /** 返回空的 {@link ActorSystemSetup} 实例。 */
    public static ActorSystemSetup empty() {
        return EMPTY;
    }

    /**
     * 使用给定 {@link Setup} 实例列表创建 {@link ActorSystemSetup}。
     *
     * @param settings Setup 实例
     * @return 不可变的 ActorSystemSetup
     */
    public static ActorSystemSetup create(Setup... settings) {
        Map<Class<?>, Object> map = new HashMap<>();
        for (Setup setting : settings) {
            Objects.requireNonNull(setting, "setting must not be null");
            map.put(setting.getClass(), setting);
        }
        return new ActorSystemSetup(map);
    }

    /**
     * 按具体类型提取 {@link Setup} 实例。
     *
     * @param clazz 目标 Setup 类型
     * @param <T> Setup 子类型
     * @return 存在时返回对应实例
     */
    @SuppressWarnings("unchecked")
    public <T extends Setup> Optional<T> get(Class<T> clazz) {
        return Optional.ofNullable((T) setups.get(clazz));
    }

    /**
     * 添加或替换同类型的 {@link Setup} 实例，返回新 {@link ActorSystemSetup}。
     *
     * @param t Setup 实例
     * @param <T> Setup 子类型
     * @return 更新后的 ActorSystemSetup
     */
    public <T extends Setup> ActorSystemSetup withSetup(T t) {
        Objects.requireNonNull(t, "setup must not be null");
        Map<Class<?>, Object> next = new HashMap<>(setups);
        next.put(t.getClass(), t);
        return new ActorSystemSetup(next);
    }

    /**
     * {@link #withSetup(Setup)} 的链式别名。
     *
     * @param t Setup 实例
     * @param <T> Setup 子类型
     * @return 更新后的 ActorSystemSetup
     */
    public <T extends Setup> ActorSystemSetup and(T t) {
        return withSetup(t);
    }

    @Override
    public String toString() {
        return "ActorSystemSettings(" + setups.keySet().stream().map(Class::getName).collect(Collectors.joining(",")) + ")";
    }
}
