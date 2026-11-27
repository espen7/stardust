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

import java.io.NotSerializableException;
import java.nio.ByteBuffer;

/**
 * 原样透传 {@code byte[]} 的内置序列化器。
 */
public class ByteArraySerializer implements BaseSerializer, ByteBufferSerializer {
    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造字节数组序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public ByteArraySerializer(ExtendedActorSystem system) {
        this.system = system;
        this.identifier = BaseSerializer.identifierFromConfig(getClass(), system);
    }

    @Override
    public ExtendedActorSystem system() {
        return system;
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
    public byte[] toBinary(Object o) {
        if (o == null) return null;
        if (o instanceof byte[] bytes) return bytes;
        throw new IllegalArgumentException(
                getClass().getName() + " only serializes byte arrays, not [" + o.getClass().getName() + "]");
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        return bytes;
    }

    @Override
    public void toBinary(Object o, ByteBuffer buf) {
        if (o == null) return;
        if (o instanceof byte[] bytes) {
            buf.put(bytes);
            return;
        }
        throw new IllegalArgumentException(
                getClass().getName() + " only serializes byte arrays, not [" + o.getClass().getName() + "]");
    }

    @Override
    public Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException {
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }
}
