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

import io.github.espen7.tapio.actor.ConfigKeys;
import io.github.espen7.tapio.annotation.InternalApi;

import java.io.NotSerializableException;

/**
 * INTERNAL API：限制嵌套载荷反序列化递归深度的线程局部守卫。
 */
@InternalApi
public final class NestedDeserialization {
    private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);

    private NestedDeserialization() {
    }

    /**
     * 可抛出 {@link NotSerializableException} 的反序列化操作函数。
     *
     * @param <T> 返回值类型
     */
    @FunctionalInterface
    public interface DeserializationSupplier<T> {
        /**
         * 执行反序列化。
         *
         * @return 反序列化结果
         * @throws NotSerializableException 反序列化失败时抛出
         */
        T get() throws NotSerializableException;
    }

    /**
     * 在当前线程嵌套深度加一后执行 {@code body}，超过 {@code max} 时抛出 {@link NotSerializableException}。
     *
     * @param max 最大允许嵌套深度
     * @param body 反序列化逻辑
     * @param <T> 返回值类型
     * @return {@code body} 返回值
     * @throws NotSerializableException 超过最大嵌套深度或底层抛出异常时
     */
    public static <T> T atNextLevel(int max, DeserializationSupplier<T> body) throws NotSerializableException {
        int[] counter = DEPTH.get();
        counter[0] += 1;
        try {
            if (counter[0] > max) {
                throw new NotSerializableException(
                        "Message exceeds the maximum deserialization nesting depth of [" + max + "]. "
                                + "Configure with '" + ConfigKeys.ACTOR_SERIALIZATION_MAX_NESTING_DEPTH + "'.");
            }
            return body.get();
        } finally {
            counter[0] -= 1;
        }
    }

    /**
     * 返回当前线程的嵌套深度（供测试使用）。
     *
     * @return 当前嵌套深度
     */
    public static int currentDepth() {
        return DEPTH.get()[0];
    }
}
