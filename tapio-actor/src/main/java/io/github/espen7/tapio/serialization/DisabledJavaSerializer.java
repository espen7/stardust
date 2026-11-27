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
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;
import io.github.espen7.tapio.util.NoStackTrace;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;

/**
 * 当 {@code tapio.actor.allow-java-serialization = off} 时替代 {@link JavaSerializer} 的禁用占位序列化器。
 */
public final class DisabledJavaSerializer implements Serializer, ByteBufferSerializer {

    /**
     * 禁用状态下尝试执行 Java 序列化或反序列化时抛出的异常。
     */
    public static final class JavaSerializationException extends RuntimeException implements NoStackTrace {
        /**
         * 构造异常。
         *
         * @param msg 错误描述
         */
        public JavaSerializationException(String msg) {
            super(msg);
        }

        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    /** 禁用状态下尝试序列化时抛出的共享异常实例。 */
    public static final JavaSerializationException IllegalSerialization = new JavaSerializationException(
            "Attempted to serialize message using Java serialization while `tapio.actor.allow-java-serialization` was disabled. Check WARNING logs for more details.");

    /** 禁用状态下尝试反序列化时抛出的共享异常实例。 */
    public static final JavaSerializationException IllegalDeserialization = new JavaSerializationException(
            "Attempted to deserialize message using Java serialization while `tapio.actor.allow-java-serialization` was disabled. Check WARNING logs for more details.");

    private final ExtendedActorSystem system;
    private final int identifier;
    private final LoggingAdapter log;

    /**
     * 构造禁用 Java 序列化器。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public DisabledJavaSerializer(ExtendedActorSystem system) {
        this.system = system;
        this.identifier = BaseSerializer.identifierFromConfig(JavaSerializer.class, system);
        this.log = Logging.getLogger(system, this, DisabledJavaSerializer.class);
    }

    /**
     * 返回关联的 {@link ExtendedActorSystem}。
     *
     * @return ActorSystem 实例
     */
    public ExtendedActorSystem system() {
        return system;
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
        log.warning(
                "Outgoing message attempted to use Java Serialization even though `tapio.actor.allow-java-serialization = off` was set! Message type was: [{}]",
                o == null ? "null" : o.getClass());
        throw IllegalSerialization;
    }

    @Override
    public byte[] toBinary(Object o) {
        toBinary(o, null);
        return new byte[0];
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        log.warning(
                "Incoming message attempted to use Java Serialization even though `tapio.actor.allow-java-serialization = off` was set!");
        throw IllegalDeserialization;
    }

    @Override
    public Object fromBinary(ByteBuffer buf, String manifest) throws NotSerializableException {
        log.warning(
                "Incoming message attempted to use Java Serialization even though `tapio.actor.allow-java-serialization = off` was set!");
        throw IllegalDeserialization;
    }
}
