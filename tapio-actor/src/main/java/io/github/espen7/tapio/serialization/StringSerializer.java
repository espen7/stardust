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

import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * INTERNAL API：原生 {@link String} 序列化器。
 */
@InternalApi
public final class StringSerializer implements Serializer, ByteBufferSerializer {

    private final int identifier;

    public StringSerializer(ExtendedActorSystem system) {
        this.identifier = BaseSerializer.identifierFromConfig("primitive-string", system);
    }

    @Override
    public int identifier() {
        return identifier;
    }

    @Override
    public boolean includeManifest() {
        return false;
    }

    @Override
    public void toBinary(Object o, ByteBuffer buf) {
        buf.put(toBinary(o));
    }

    @Override
    public Object fromBinary(ByteBuffer buf, String manifest) {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] toBinary(Object o) {
        return ((String) o).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
