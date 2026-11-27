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
package io.github.espen7.tapio.cluster;

import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serial;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * INTERNAL API：基于逻辑计数器的向量时钟，用于判断 Gossip 版本的偏序与并发关系。
 */
@InternalApi
public final class VectorClock implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** 向量时钟偏序比较结果。 */
    public enum Ordering {
        After, Before, Same, Concurrent
    }

    /** 空向量时钟。 */
    public static final VectorClock EMPTY = new VectorClock(Collections.emptySortedMap());

    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private final SortedMap<String, Long> versions;

    /**
     * 构造向量时钟。
     *
     * @param versions 节点哈希到版本号的有序映射
     */
    public VectorClock(SortedMap<String, Long> versions) {
        this.versions = Collections.unmodifiableSortedMap(new TreeMap<>(Objects.requireNonNull(versions, "versions")));
    }

    /**
     * 计算节点名称的 MD5 哈希键。
     *
     * @param name 节点标识字符串
     * @return MD5 十六进制哈希
     */
    public static String node(String name) {
        try {
            MessageDigest digester = MessageDigest.getInstance("MD5");
            digester.update(name.getBytes(StandardCharsets.UTF_8));
            return HEX_FORMAT.formatHex(digester.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 返回底层的有序版本映射。
     *
     * @return 不可变版本映射
     */
    public SortedMap<String, Long> versions() {
        return versions;
    }

    /**
     * 递增指定节点的逻辑时钟并返回新 {@link VectorClock}。
     *
     * @param node 节点哈希键
     * @return 递增后的向量时钟
     */
    public VectorClock increment(String node) {
        TreeMap<String, Long> next = new TreeMap<>(versions);
        next.put(node, next.getOrDefault(node, 0L) + 1L);
        return new VectorClock(next);
    }

    /**
     * 判断当前时钟是否严格并发于 {@code that}。
     *
     * @param that 另一向量时钟
     * @return 并发时返回 {@code true}
     */
    public boolean isConcurrentWith(VectorClock that) {
        return compareTo(that) == Ordering.Concurrent;
    }

    /**
     * 判断当前时钟是否严格早于 {@code that}。
     *
     * @param that 另一向量时钟
     * @return 早于返回 {@code true}
     */
    public boolean isBefore(VectorClock that) {
        return compareTo(that) == Ordering.Before;
    }

    /**
     * 判断当前时钟是否严格晚于 {@code that}。
     *
     * @param that 另一向量时钟
     * @return 晚于返回 {@code true}
     */
    public boolean isAfter(VectorClock that) {
        return compareTo(that) == Ordering.After;
    }

    /**
     * 比较两个向量时钟的偏序关系。
     *
     * @param that 另一向量时钟
     * @return {@link Ordering}
     */
    public Ordering compareTo(VectorClock that) {
        Iterator<Map.Entry<String, Long>> i1 = this.versions.entrySet().iterator();
        Iterator<Map.Entry<String, Long>> i2 = that.versions.entrySet().iterator();
        Map.Entry<String, Long> e1 = i1.hasNext() ? i1.next() : null;
        Map.Entry<String, Long> e2 = i2.hasNext() ? i2.next() : null;
        Ordering current = Ordering.Same;

        while (e1 != null || e2 != null) {
            if (e1 == null) {
                current = (current == Ordering.After) ? Ordering.Concurrent : Ordering.Before;
                e2 = null;
                break;
            }
            if (e2 == null) {
                current = (current == Ordering.Before) ? Ordering.Concurrent : Ordering.After;
                e1 = null;
                break;
            }
            int keyCmp = e1.getKey().compareTo(e2.getKey());
            if (keyCmp == 0) {
                int valCmp = Long.compare(e1.getValue(), e2.getValue());
                if (valCmp < 0) {
                    if (current == Ordering.After) return Ordering.Concurrent;
                    current = Ordering.Before;
                } else if (valCmp > 0) {
                    if (current == Ordering.Before) return Ordering.Concurrent;
                    current = Ordering.After;
                }
                e1 = i1.hasNext() ? i1.next() : null;
                e2 = i2.hasNext() ? i2.next() : null;
            } else if (keyCmp < 0) {
                if (current == Ordering.Before) return Ordering.Concurrent;
                current = Ordering.After;
                e1 = i1.hasNext() ? i1.next() : null;
            } else {
                if (current == Ordering.After) return Ordering.Concurrent;
                current = Ordering.Before;
                e2 = i2.hasNext() ? i2.next() : null;
            }
        }
        return current;
    }

    /**
     * 合并两个向量时钟，取每个节点版本的最大值。
     *
     * @param that 另一向量时钟
     * @return 合并后的新向量时钟
     */
    public VectorClock merge(VectorClock that) {
        TreeMap<String, Long> merged = new TreeMap<>(this.versions);
        for (Map.Entry<String, Long> entry : that.versions.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return new VectorClock(merged);
    }

    /**
     * 移除已下线节点的时钟条目以裁剪元数据。
     *
     * @param removedNode 节点哈希键
     * @return 裁剪后的向量时钟
     */
    public VectorClock prune(String removedNode) {
        if (!versions.containsKey(removedNode)) {
            return this;
        }
        TreeMap<String, Long> next = new TreeMap<>(versions);
        next.remove(removedNode);
        return new VectorClock(next);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof VectorClock other && versions.equals(other.versions);
    }

    @Override
    public int hashCode() {
        return versions.hashCode();
    }

    @Override
    public String toString() {
        return "VectorClock(" + versions + ")";
    }
}
