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

import io.github.espen7.tapio.actor.AbstractExtensionId;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.actor.ExtensionId;
import io.github.espen7.tapio.actor.ExtensionIdProvider;

/**
 * 提供 {@link Serialization} 扩展访问的 {@link ExtensionId} 与 {@link ExtensionIdProvider}。
 */
public final class SerializationExtension extends AbstractExtensionId<Serialization> implements ExtensionIdProvider {

    /** 单例实例。 */
    public static final SerializationExtension INSTANCE = new SerializationExtension();

    private SerializationExtension() {
    }

    @Override
    public ExtensionId<Serialization> lookup() {
        return INSTANCE;
    }

    @Override
    public Serialization createExtension(ExtendedActorSystem system) {
        return new Serialization(system);
    }
}
