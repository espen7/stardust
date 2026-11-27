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
import io.github.espen7.tapio.util.ClassLoaderObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.Callable;

/**
 * 基于标准 Java 对象流的序列化器。
 */
public class JavaSerializer implements BaseSerializer {

    /**
     * 在线程局部变量中持有当前 {@link ExtendedActorSystem}，供自定义 {@code readObject}/{@code writeObject} 使用。
     */
    public static final CurrentSystem currentSystem = new CurrentSystem();

    /**
     * 线程局部系统上下文容器。
     */
    public static final class CurrentSystem {
        private final ThreadLocal<ExtendedActorSystem> tl = new ThreadLocal<>();

        private CurrentSystem() {
        }

        /**
         * 返回当前线程绑定的 {@link ExtendedActorSystem}，未绑定时返回 {@code null}。
         *
         * @return 当前系统或 {@code null}
         */
        public ExtendedActorSystem value() {
            return tl.get();
        }

        /**
         * 在指定 {@link ExtendedActorSystem} 绑定上下文中执行回调。
         *
         * @param value 要绑定的系统
         * @param callable 要执行的操作
         * @param <S> 返回类型
         * @return 回调返回值
         * @throws Exception 回调抛出的异常
         */
        public <S> S withValue(ExtendedActorSystem value, Callable<S> callable) throws Exception {
            ExtendedActorSystem prev = tl.get();
            tl.set(value);
            try {
                return callable.call();
            } finally {
                if (prev == null) {
                    tl.remove();
                } else {
                    tl.set(prev);
                }
            }
        }
    }

    private final ExtendedActorSystem system;
    private final int identifier;

    /**
     * 构造 Java 序列化器；若配置禁用 Java 序列化则立即抛出 {@link DisabledJavaSerializer.JavaSerializationException}。
     *
     * @param system 关联的 {@link ExtendedActorSystem}
     */
    public JavaSerializer(ExtendedActorSystem system) {
        if (!system.settings().AllowJavaSerialization) {
            throw new DisabledJavaSerializer.JavaSerializationException(
                    "Attempted creation of `JavaSerializer` while `tapio.actor.allow-java-serialization = off` was set!");
        }
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
        return false;
    }

    @Override
    public byte[] toBinary(Object o) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            currentSystem.withValue(system, () -> {
                out.writeObject(o);
                return null;
            });
            out.close();
            return bos.toByteArray();
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object fromBinary(byte[] bytes, Class<?> manifest) throws NotSerializableException {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(
                system.dynamicAccess().classLoader(), new ByteArrayInputStream(bytes))) {
            return currentSystem.withValue(system, in::readObject);
        } catch (NotSerializableException e) {
            throw e;
        } catch (Exception e) {
            NotSerializableException nse = new NotSerializableException(
                    "Java deserialization failed: " + e.getMessage());
            nse.initCause(e);
            throw nse;
        }
    }
}
