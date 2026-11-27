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
 * INTERNAL API：原生 {@link Boolean} 序列化器。
 */
@InternalApi
public final class BooleanSerializer implements Serializer, ByteBufferSerializer {

    private static final byte FALSE_BYTE = 0;
    private static final byte TRUE_BYTE = 1;

    private final int identifier;

    public BooleanSerializer(ExtendedActorSystem system) {
        this.identifier = BaseSerializer.identifierFromConfig("primitive-boolean", system);
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
        buf.put(encodeFlag(o));
    }

    @Override
    public Object fromBinary(ByteBuffer buf, String manifest) {
        return decodeFlag(buf.get());
    }

    @Override
    public byte[] toBinary(Object o) {
        return new byte[] { encodeFlag(o) };
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) {
        return decodeFlag(bytes[0]);
    }

    private static byte encodeFlag(Object o) {
        if (Boolean.TRUE.equals(o)) {
            return TRUE_BYTE;
        }
        if (Boolean.FALSE.equals(o)) {
            return FALSE_BYTE;
        }
        throw new IllegalArgumentException("Non boolean flag: " + o);
    }

    private static Boolean decodeFlag(byte b) {
        if (b == TRUE_BYTE) {
            return Boolean.TRUE;
        }
        if (b == FALSE_BYTE) {
            return Boolean.FALSE;
        }
        throw new IllegalArgumentException("Non boolean flag byte: " + b);
    }
}
