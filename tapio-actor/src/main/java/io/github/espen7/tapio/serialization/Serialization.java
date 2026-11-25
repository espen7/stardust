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
package io.github.espen7.tapio.serialization;

import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.Address;

/** 本地 ActorRef 序列化所需的运行时信息。 */
public final class Serialization {
    private Serialization() {
    }

    /**
     * 保存序列化时的系统和地址。
     *
     * @param address ActorSystem 的默认地址
     * @param system 关联的 ActorSystem
     */
    public record Information(Address address, ActorSystem system) {
    }
}
