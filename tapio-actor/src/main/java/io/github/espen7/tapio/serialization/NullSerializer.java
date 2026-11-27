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

import java.io.NotSerializableException;

/**
 * 专门用于序列化和反序列化 {@code null} 的内置序列化器。
 */
public final class NullSerializer implements Serializer {

    /** 单例实例。 */
    public static final NullSerializer INSTANCE = new NullSerializer();

    private static final byte[] NULL_AS_BYTES = new byte[0];

    private NullSerializer() {
    }

    @Override
    public int identifier() {
        return 0;
    }

    @Override
    public boolean includeManifest() {
        return false;
    }

    @Override
    public byte[] toBinary(Object o) {
        return NULL_AS_BYTES;
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        if (bytes != null && bytes.length != 0) {
            throw new NotSerializableException(
                    getClass().getName() + " (serializer id [" + identifier()
                            + "]) only deserializes empty input, got [" + bytes.length + "] bytes.");
        }
        return null;
    }
}
