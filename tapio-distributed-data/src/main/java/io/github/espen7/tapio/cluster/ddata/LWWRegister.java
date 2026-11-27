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
package io.github.espen7.tapio.cluster.ddata;

import io.github.espen7.tapio.cluster.UniqueAddress;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 最后写入胜出寄存器（Last-Writer-Wins Register）CRDT。
 *
 * @param <A> 存储值类型
 */
public final class LWWRegister<A> implements ReplicatedData, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 时间戳时钟函数接口。 */
    @FunctionalInterface
    public interface Clock<A> {
        /**
         * 计算下一次写入的时间戳。
         *
         * @param currentTimestamp 当前寄存器时间戳
         * @param value 新写入的值
         * @return 新的单调时间戳
         */
        long apply(long currentTimestamp, A value);
    }

    /** 默认时钟：取 {@code max(System.currentTimeMillis(), currentTimestamp + 1)}。 */
    public static <A> Clock<A> defaultClock() {
        return (currentTimestamp, value) -> Math.max(System.currentTimeMillis(), currentTimestamp + 1);
    }

    private final UniqueAddress node;
    private final A value;
    private final long timestamp;

    /**
     * 构造 LWWRegister 实例。
     *
     * @param node 更新节点唯一地址
     * @param value 值
     * @param timestamp 时间戳
     */
    public LWWRegister(UniqueAddress node, A value, long timestamp) {
        this.node = Objects.requireNonNull(node, "node");
        this.value = value;
        this.timestamp = timestamp;
    }

    /** 创建初始 LWWRegister。 */
    public static <A> LWWRegister<A> create(SelfUniqueAddress node, A value) {
        return new LWWRegister<>(node.uniqueAddress(), value, LWWRegister.<A>defaultClock().apply(0L, value));
    }

    public UniqueAddress updatedBy() {
        return node;
    }

    public A value() {
        return value;
    }

    public A getValue() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    /** 使用新值与默认时钟更新寄存器。 */
    public LWWRegister<A> withValue(SelfUniqueAddress self, A newValue) {
        return withValue(self, newValue, defaultClock());
    }

    /** 使用新值与指定时钟更新寄存器。 */
    public LWWRegister<A> withValue(SelfUniqueAddress self, A newValue, Clock<A> clock) {
        return new LWWRegister<>(self.uniqueAddress(), newValue, clock.apply(this.timestamp, newValue));
    }

    @Override
    @SuppressWarnings("unchecked")
    public LWWRegister<A> mergeData(ReplicatedData that) {
        LWWRegister<A> other = (LWWRegister<A>) that;
        if (other.timestamp > this.timestamp) {
            return other;
        }
        if (other.timestamp < this.timestamp) {
            return this;
        }
        return other.node.compareTo(this.node) < 0 ? other : this;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LWWRegister<?> other
                && timestamp == other.timestamp
                && node.equals(other.node)
                && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, value, timestamp);
    }

    @Override
    public String toString() {
        return "LWWRegister(" + value + ", " + timestamp + ", " + node + ")";
    }
}
