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
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 分布式数据 CRDT 使用的版本向量。
 */
public final class VersionVector implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 版本向量偏序比较结果。 */
    public enum Ordering {
        After, Before, Same, Concurrent
    }

    /** 空版本向量。 */
    public static final VersionVector EMPTY = new VersionVector(Collections.emptySortedMap());

    private final SortedMap<UniqueAddress, Long> versions;

    /**
     * 构造版本向量。
     *
     * @param versions 节点到逻辑计数器的有序映射
     */
    public VersionVector(SortedMap<UniqueAddress, Long> versions) {
        this.versions = Collections.unmodifiableSortedMap(new TreeMap<>(versions));
    }

    public SortedMap<UniqueAddress, Long> versions() {
        return versions;
    }

    public long versionAt(UniqueAddress node) {
        return versions.getOrDefault(node, 0L);
    }

    public VersionVector increment(UniqueAddress node) {
        TreeMap<UniqueAddress, Long> next = new TreeMap<>(versions);
        next.put(node, next.getOrDefault(node, 0L) + 1L);
        return new VersionVector(next);
    }

    public VersionVector merge(VersionVector that) {
        TreeMap<UniqueAddress, Long> merged = new TreeMap<>(this.versions);
        for (Map.Entry<UniqueAddress, Long> e : that.versions.entrySet()) {
            merged.merge(e.getKey(), e.getValue(), Math::max);
        }
        return new VersionVector(merged);
    }

    public Ordering compareTo(VersionVector that) {
        boolean thisGreater = false;
        boolean thatGreater = false;
        for (Map.Entry<UniqueAddress, Long> e : this.versions.entrySet()) {
            long otherVal = that.versions.getOrDefault(e.getKey(), 0L);
            if (e.getValue() > otherVal) {
                thisGreater = true;
            } else if (e.getValue() < otherVal) {
                thatGreater = true;
            }
            if (thisGreater && thatGreater) {
                return Ordering.Concurrent;
            }
        }
        for (Map.Entry<UniqueAddress, Long> e : that.versions.entrySet()) {
            if (!this.versions.containsKey(e.getKey()) && e.getValue() > 0L) {
                thatGreater = true;
                if (thisGreater) {
                    return Ordering.Concurrent;
                }
            }
        }
        if (thisGreater) return Ordering.After;
        if (thatGreater) return Ordering.Before;
        return Ordering.Same;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof VersionVector other && versions.equals(other.versions);
    }

    @Override
    public int hashCode() {
        return versions.hashCode();
    }

    @Override
    public String toString() {
        return "VersionVector(" + versions + ")";
    }
}
