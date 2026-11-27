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
package io.github.espen7.tapio.remote.serialization;

import com.google.protobuf.MessageLite;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.serialization.BaseSerializer;

import java.io.NotSerializableException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 Google Protobuf {@link MessageLite} 的序列化器。
 */
public class ProtobufSerializer implements BaseSerializer {
    private final ExtendedActorSystem system;
    private final int identifier;
    private final ConcurrentHashMap<Class<?>, Method> parseFromCache = new ConcurrentHashMap<>();

    /**
     * 构造 Protobuf 序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public ProtobufSerializer(ExtendedActorSystem system) {
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
        return true;
    }

    @Override
    public byte[] toBinary(Object o) {
        if (o instanceof MessageLite msg) {
            return msg.toByteArray();
        }
        throw new IllegalArgumentException(
                "Can't serialize a non-protobuf message using protobuf [" + (o == null ? "null" : o) + "]");
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        if (manifest == null) {
            throw new IllegalArgumentException("Need a protobuf message class to be able to serialize bytes using protobuf");
        }
        try {
            Method parseFrom = parseFromCache.computeIfAbsent(manifest, clazz -> {
                try {
                    return clazz.getDeclaredMethod("parseFrom", byte[].class);
                } catch (NoSuchMethodException e) {
                    throw new IllegalArgumentException(
                            "Protobuf message [" + clazz.getName() + "] has no parseFrom(byte[]) method", e);
                }
            });
            return parseFrom.invoke(null, (Object) bytes);
        } catch (Exception e) {
            NotSerializableException nse = new NotSerializableException(
                    "Cannot deserialize protobuf message [" + manifest.getName() + "]: " + e.getMessage());
            nse.initCause(e);
            throw nse;
        }
    }
}
