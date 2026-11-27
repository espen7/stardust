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

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * 分布式数据条目的类型化键。
 *
 * @param <T> 关联的 CRDT 数据类型
 */
public abstract class Key<T extends ReplicatedData> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String id;

    /**
     * 构造键实例。
     *
     * @param id 唯一键标识符
     */
    protected Key(String id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    /** 返回键标识符。 */
    public final String id() {
        return id;
    }

    @Override
    public final int hashCode() {
        return id.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof Key<?> other && id.equals(other.id);
    }

    @Override
    public String toString() {
        return id;
    }
}
