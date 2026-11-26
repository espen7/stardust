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

/**
 * 用于通过配置加载 {@link ExtensionId} 的提供者接口，对应 Pekko {@code ExtensionIdProvider}。
 *
 * <p>当通过 {@code tapio.extensions} 或 {@code tapio.library-extensions} 配置加载扩展时，
 * 配置类可实现此接口并在 {@link #lookup()} 中返回规范的 {@link ExtensionId} 单例。</p>
 */
public interface ExtensionIdProvider {
    /**
     * 返回此扩展对应的规范 {@link ExtensionId}。
     *
     * @return 规范的 ExtensionId 实例
     */
    ExtensionId<? extends Extension> lookup();
}
