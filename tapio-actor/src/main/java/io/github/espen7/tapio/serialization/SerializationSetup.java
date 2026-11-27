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
import io.github.espen7.tapio.actor.setup.Setup;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 序列化子系统的编程式配置入口。
 */
public final class SerializationSetup extends Setup {
    private final Function<ExtendedActorSystem, List<SerializerDetails>> createSerializers;

    private SerializationSetup(Function<ExtendedActorSystem, List<SerializerDetails>> createSerializers) {
        this.createSerializers = Objects.requireNonNull(createSerializers, "createSerializers");
    }

    /**
     * 创建编程式序列化配置。
     *
     * @param createSerializers 接收 {@link ExtendedActorSystem} 并返回 {@link SerializerDetails} 列表的工厂函数
     * @return {@link SerializationSetup} 实例
     */
    public static SerializationSetup create(Function<ExtendedActorSystem, List<SerializerDetails>> createSerializers) {
        return new SerializationSetup(createSerializers);
    }

    /**
     * 生成与指定系统关联的序列化器描述列表。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     * @return 序列化器描述列表
     */
    public List<SerializerDetails> createSerializers(ExtendedActorSystem system) {
        return List.copyOf(createSerializers.apply(system));
    }
}
