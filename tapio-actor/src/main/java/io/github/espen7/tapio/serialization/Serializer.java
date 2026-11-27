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
 * 对象与字节数组之间的双向序列化契约。
 */
public interface Serializer {

    /**
     * 返回此序列化器实现的全局唯一标识符（0 到 40 保留给 Tapio 内部使用）。
     *
     * @return 唯一整数 ID
     */
    int identifier();

    /**
     * 将对象序列化为字节数组；返回的数组在返回后不得再被修改。
     *
     * @param o 待序列化对象
     * @return 字节数组
     */
    byte[] toBinary(Object o);

    /**
     * 返回反序列化方法是否需要 manifest 类型提示。
     *
     * @return 需要类型提示时返回 {@code true}
     */
    boolean includeManifest();

    /**
     * 从字节数组反序列化对象，可附带可选类型提示。
     *
     * @param bytes 字节数组
     * @param manifest 可选类型提示，可为 {@code null}
     * @return 反序列化后的对象
     * @throws NotSerializableException 反序列化失败时抛出
     */
    Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException;

    /**
     * 无类型提示的反序列化便捷方法。
     *
     * @param bytes 字节数组
     * @return 反序列化后的对象
     * @throws NotSerializableException 反序列化失败时抛出
     */
    default Object fromBinary(byte[] bytes) throws NotSerializableException {
        return fromBinary(bytes, null);
    }
}
