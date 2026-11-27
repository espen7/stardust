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

import java.util.Objects;

/**
 * 标识一个 {@link Extension}。
 *
 * <p>扩展按 {@code ExtensionId} 实例的引用相等性查找，因此调用方通常应使用全局共享的
 * {@link AbstractExtensionId} 单例，避免重复加载同一扩展。</p>
 *
 * @param <T> 扩展实例类型
 */
public interface ExtensionId<T extends Extension> {
    /**
     * 获取或注册此 {@code ExtensionId} 对应的扩展实例。
     *
     * @param system 目标 ActorSystem
     * @return 扩展实例
     */
    default T apply(ActorSystem system) {
        return Objects.requireNonNull(system, "system must not be null!").registerExtension(this);
    }

    /**
     * Java API：获取或注册此 {@code ExtensionId} 对应的扩展实例。
     *
     * @param system 目标 ActorSystem
     * @return 扩展实例
     */
    default T get(ActorSystem system) {
        return apply(system);
    }

    /**
     * 由 Tapio runtime 在首次注册该扩展时调用，用于创建扩展实例。
     *
     * @param system 扩展所绑定的 ExtendedActorSystem
     * @return 新建的扩展实例（不可为 {@code null}）
     */
    T createExtension(ExtendedActorSystem system);
}
