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

/**
 * INTERNAL API：原生 {@link Long} 序列化器。
 */
@InternalApi
public final class LongSerializer implements Serializer, ByteBufferSerializer {

    private final int identifier;

    public LongSerializer(ExtendedActorSystem system) {
        this.identifier = BaseSerializer.identifierFromConfig("primitive-long", system);
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
        buf.putLong((Long) o);
    }

    @Override
    public Object fromBinary(ByteBuffer buf, String manifest) {
        return buf.getLong();
    }

    @Override
    public byte[] toBinary(Object o) {
        byte[] result = new byte[8];
        long value = (Long) o;
        int i = 0;
        while (value != 0) {
            result[i] = (byte) (value & 0xFF);
            i += 1;
            value >>>= 8;
        }
        return result;
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) {
        long result = 0L;
        int i = 7;
        while (i >= 0) {
            result <<= 8;
            result |= (bytes[i] & 0xFFL);
            i -= 1;
        }
        return result;
    }
}
