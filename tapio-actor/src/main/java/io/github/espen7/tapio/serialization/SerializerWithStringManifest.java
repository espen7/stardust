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
 * 使用字符串 manifest（而非 {@link Class}）进行类型提示的可演进序列化器基类。
 */
public abstract class SerializerWithStringManifest implements Serializer {

    @Override
    public final boolean includeManifest() {
        return true;
    }

    /**
     * 返回将传递给 {@link #fromBinary(byte[], String)} 的 manifest 字符串；不需要时返回 {@code ""}。
     *
     * @param o 待序列化对象
     * @return manifest 字符串
     */
    public abstract String manifest(Object o);

    /**
     * 使用字符串 manifest 从字节数组反序列化对象。
     *
     * @param bytes 字节数组
     * @param manifest manifest 类型提示
     * @return 反序列化后的对象
     * @throws NotSerializableException 未知 manifest 或反序列化失败时抛出
     */
    public abstract Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException;

    @Override
    public final Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        String manifestString = manifest != null ? manifest.getName() : "";
        return fromBinary(bytes, manifestString);
    }
}
