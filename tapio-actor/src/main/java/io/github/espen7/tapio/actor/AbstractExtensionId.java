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
 * {@link ExtensionId} 的 Java 基类，固定使用引用相等性与 {@link System#identityHashCode}。
 *
 * @param <T> 扩展实例类型
 */
public abstract class AbstractExtensionId<T extends Extension> implements ExtensionId<T> {
    /** 创建 AbstractExtensionId 实例。 */
    protected AbstractExtensionId() {
    }

    @Override
    public final int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public final boolean equals(Object other) {
        return this == other;
    }
}
