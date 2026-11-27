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

import java.util.List;
import java.util.Objects;

/**
 * 编程式注册的序列化器及其绑定类列表的描述信息。
 */
public final class SerializerDetails {
    private final String alias;
    private final Serializer serializer;
    private final List<Class<?>> useFor;

    private SerializerDetails(String alias, Serializer serializer, List<Class<?>> useFor) {
        this.alias = Objects.requireNonNull(alias, "alias");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.useFor = List.copyOf(Objects.requireNonNull(useFor, "useFor"));
    }

    /**
     * 创建编程式序列化器描述。
     *
     * @param alias 序列化器别名
     * @param serializer 序列化器实例
     * @param useFor 绑定到该序列化器的类或接口列表
     * @return {@link SerializerDetails} 实例
     */
    public static SerializerDetails create(String alias, Serializer serializer, List<Class<?>> useFor) {
        return new SerializerDetails(alias, serializer, useFor);
    }

    /**
     * 返回序列化器别名。
     *
     * @return 别名
     */
    public String alias() {
        return alias;
    }

    /**
     * 返回序列化器实例。
     *
     * @return 序列化器
     */
    public Serializer serializer() {
        return serializer;
    }

    /**
     * 返回绑定的类列表。
     *
     * @return 不可变类列表
     */
    public List<Class<?>> useFor() {
        return useFor;
    }
}
