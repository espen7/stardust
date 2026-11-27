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
import java.nio.ByteBuffer;

/**
 * 基于 {@link ByteBuffer} 的零拷贝序列化扩展接口。
 */
public interface ByteBufferSerializer {

    /**
     * 将对象序列化写入给定的 {@link ByteBuffer}。
     *
     * @param o 待序列化对象
     * @param buf 目标缓冲区
     */
    void toBinary(Object o, ByteBuffer buf);

    /**
     * 从给定的 {@link ByteBuffer} 与 manifest 反序列化对象。
     *
     * @param buf 源缓冲区
     * @param manifest manifest 类型提示
     * @return 反序列化后的对象
     * @throws NotSerializableException 反序列化失败时抛出
     */
    Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException;
}
