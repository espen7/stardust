/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.serialization;

import io.github.espen7.tapio.actor.ConfigKeys;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;

/**
 * 从配置 {@code tapio.actor.serialization-identifiers} 读取唯一 ID 的序列化器契约。
 */
public interface BaseSerializer extends Serializer {

    /**
     * 序列化器唯一 ID 的配置根路径。
     */
    String SerializationIdentifiers = ConfigKeys.ACTOR_SERIALIZATION_IDENTIFIERS;

    /**
     * 返回关联的 {@link ExtendedActorSystem}。
     *
     * @return ActorSystem 实例
     */
    ExtendedActorSystem system();

    /**
     * INTERNAL API：从配置读取给定序列化器类的唯一 ID。
     *
     * @param clazz 序列化器类
     * @param system 关联的 ActorSystem
     * @return 配置的唯一 ID
     */
    @InternalApi
    static int identifierFromConfig(Class<?> clazz, ExtendedActorSystem system) {
        return system.settings().config().getInt(SerializationIdentifiers + ".\"" + clazz.getName() + "\"");
    }

    /**
     * INTERNAL API：从配置读取给定绑定名的唯一 ID。
     *
     * @param bindingName 绑定名
     * @param system 关联的 ActorSystem
     * @return 配置的唯一 ID
     */
    @InternalApi
    static int identifierFromConfig(String bindingName, ExtendedActorSystem system) {
        return system.settings().config().getInt(SerializationIdentifiers + ".\"" + bindingName + "\"");
    }
}
