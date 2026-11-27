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
 * 标记一个可注册到 {@link ActorSystem} 的扩展组件。
 *
 * <p>扩展实例由对应的 {@link ExtensionId#createExtension(ExtendedActorSystem)} 创建，并在
 * 同一个 {@link ActorSystem} 内按 {@link ExtensionId} 的对象标识保持单例。</p>
 */
public interface Extension {
}
