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
package io.github.espen7.tapio.remote;

import com.google.protobuf.ByteString;
import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.serialization.Serialization;
import io.github.espen7.tapio.serialization.SerializationExtension;
import io.github.espen7.tapio.serialization.Serializer;
import io.github.espen7.tapio.serialization.Serializers;
import io.github.espen7.tapio.util.NonFatal;

import java.io.NotSerializableException;

/**
 * INTERNAL API：使用 {@link SerializationExtension} 将消息与 {@link WireFormats.SerializedMessage} 互相转换的辅助类。
 */
@InternalApi
public final class MessageSerializer {
    private MessageSerializer() {
    }

    /**
     * 序列化器内部执行 {@code toBinary} 失败时包装抛出的运行时异常。
     */
    public static final class SerializationException extends RuntimeException {
        /**
         * 构造序列化异常。
         *
         * @param msg 错误描述
         * @param cause 根因异常
         */
        public SerializationException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /**
     * 将 {@link WireFormats.SerializedMessage} 反序列化为消息对象。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     * @param messageProtocol 线协议消息信封
     * @return 反序列化得到的对象
     * @throws NotSerializableException 反序列化失败时抛出
     */
    public static Object deserialize(ExtendedActorSystem system, WireFormats.SerializedMessage messageProtocol)
            throws NotSerializableException {
        String manifest = (messageProtocol.messageManifest() != null && !messageProtocol.messageManifest().isEmpty())
                ? messageProtocol.messageManifest().toStringUtf8()
                : "";
        return SerializationExtension.INSTANCE.get(system).deserialize(
                messageProtocol.message().toByteArray(),
                messageProtocol.serializerId(),
                manifest);
    }

    /**
     * 将消息对象序列化为 {@link WireFormats.SerializedMessage}。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     * @param message 待序列化消息
     * @return 线协议消息信封
     * @throws NotSerializableException 未找到匹配的序列化器时抛出
     */
    public static WireFormats.SerializedMessage serialize(ExtendedActorSystem system, Object message)
            throws NotSerializableException {
        Serialization s = SerializationExtension.INSTANCE.get(system);
        Serializer serializer = s.findSerializerFor(message);
        Serialization.Information oldInfo = Serialization.currentTransportInformation.get();
        if (oldInfo == null) {
            Serialization.currentTransportInformation.set(s.serializationInformation());
        }
        try {
            byte[] bytes = serializer.toBinary(message);
            String manifest = Serializers.manifestFor(serializer, message);
            ByteString manifestBytes = (manifest == null || manifest.isEmpty())
                    ? ByteString.EMPTY
                    : ByteString.copyFromUtf8(manifest);
            return new WireFormats.SerializedMessage(
                    bytes == null ? ByteString.EMPTY : ByteString.copyFrom(bytes),
                    serializer.identifier(),
                    manifestBytes);
        } catch (Throwable t) {
            if (!NonFatal.apply(t)) throw (Error) t;
            throw new SerializationException(
                    "Failed to serialize remote message [" + (message == null ? "null" : message.getClass())
                            + "] using serializer [" + serializer.getClass() + "].",
                    t);
        } finally {
            if (oldInfo == null) {
                Serialization.currentTransportInformation.remove();
            } else {
                Serialization.currentTransportInformation.set(oldInfo);
            }
        }
    }
}
