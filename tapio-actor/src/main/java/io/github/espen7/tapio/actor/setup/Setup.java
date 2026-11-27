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

/**
 * 可放入 {@link ActorSystemSetup} 的编程式配置项基类。
 */
public abstract class Setup {
    /** 创建 Setup 实例。 */
    protected Setup() {
    }

    /**
     * 将当前配置项与另一个 {@link Setup} 组合为 {@link ActorSystemSetup}。
     *
     * @param other 另一个 Setup 实例
     * @return 组合后的 ActorSystemSetup
     */
    public final ActorSystemSetup and(Setup other) {
        return ActorSystemSetup.create(this, other);
    }
}
