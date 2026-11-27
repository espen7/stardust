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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 观察-移除集合（Observed-Remove Set）CRDT，支持在分布式并发场景下安全添加与移除元素（添加优先语义）。
 *
 * @param <A> 集合元素类型
 */
public final class ORSet<A> implements ReplicatedData, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Map<A, VersionVector> elementsMap;
    private final VersionVector vvector;

    /**
     * 构造 ORSet。
     *
     * @param elementsMap 元素到其点（dot）版本向量的映射
     * @param vvector 集合全局版本向量
     */
    public ORSet(Map<A, VersionVector> elementsMap, VersionVector vvector) {
        this.elementsMap = Map.copyOf(Objects.requireNonNull(elementsMap, "elementsMap"));
        this.vvector = Objects.requireNonNull(vvector, "vvector");
    }

    /** 返回空 {@link ORSet}。 */
    public static <A> ORSet<A> create() {
        return new ORSet<>(Map.of(), VersionVector.EMPTY);
    }

    /** 返回当前元素不可变集合。 */
    public Set<A> elements() {
        return elementsMap.keySet();
    }

    /** Java API：返回当前元素不可变集合。 */
    public Set<A> getElements() {
        return elementsMap.keySet();
    }

    public boolean contains(A elem) {
        return elementsMap.containsKey(elem);
    }

    public boolean isEmpty() {
        return elementsMap.isEmpty();
    }

    public int size() {
        return elementsMap.size();
    }

    public Map<A, VersionVector> elementsMap() {
        return elementsMap;
    }

    public VersionVector vvector() {
        return vvector;
    }

    /** 添加元素并返回新的 {@link ORSet}。 */
    public ORSet<A> add(SelfUniqueAddress node, A element) {
        UniqueAddress ua = node.uniqueAddress();
        VersionVector nextVVector = vvector.increment(ua);
        long nextDot = nextVVector.versionAt(ua);
        VersionVector dot = VersionVector.EMPTY.increment(ua);
        VersionVector existing = elementsMap.get(element);
        VersionVector elemDot = existing == null
                ? new VersionVector(new java.util.TreeMap<>(Map.of(ua, nextDot)))
                : mergeSingleDot(existing, ua, nextDot);
        Map<A, VersionVector> nextMap = new HashMap<>(elementsMap);
        nextMap.put(element, elemDot);
        return new ORSet<>(nextMap, nextVVector);
    }

    private static VersionVector mergeSingleDot(VersionVector existing, UniqueAddress ua, long dot) {
        java.util.TreeMap<UniqueAddress, Long> map = new java.util.TreeMap<>(existing.versions());
        map.put(ua, dot);
        return new VersionVector(map);
    }

    /** 移除元素并返回新的 {@link ORSet}。 */
    public ORSet<A> remove(SelfUniqueAddress node, A element) {
        if (!elementsMap.containsKey(element)) {
            return this;
        }
        Map<A, VersionVector> nextMap = new HashMap<>(elementsMap);
        nextMap.remove(element);
        return new ORSet<>(nextMap, vvector);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ORSet<A> mergeData(ReplicatedData that) {
        ORSet<A> other = (ORSet<A>) that;
        Set<A> allKeys = new HashSet<>(this.elementsMap.keySet());
        allKeys.addAll(other.elementsMap.keySet());
        Map<A, VersionVector> nextMap = new HashMap<>();

        for (A key : allKeys) {
            VersionVector d1 = this.elementsMap.get(key);
            VersionVector d2 = other.elementsMap.get(key);
            java.util.TreeMap<UniqueAddress, Long> surviving = new java.util.TreeMap<>();

            if (d1 != null) {
                for (Map.Entry<UniqueAddress, Long> e : d1.versions().entrySet()) {
                    UniqueAddress n = e.getKey();
                    long v = e.getValue();
                    if ((d2 != null && d2.versionAt(n) >= v) || other.vvector.versionAt(n) < v) {
                        surviving.merge(n, v, Math::max);
                    }
                }
            }
            if (d2 != null) {
                for (Map.Entry<UniqueAddress, Long> e : d2.versions().entrySet()) {
                    UniqueAddress n = e.getKey();
                    long v = e.getValue();
                    if ((d1 != null && d1.versionAt(n) >= v) || this.vvector.versionAt(n) < v) {
                        surviving.merge(n, v, Math::max);
                    }
                }
            }
            if (!surviving.isEmpty()) {
                nextMap.put(key, new VersionVector(surviving));
            }
        }
        return new ORSet<>(Collections.unmodifiableMap(nextMap), this.vvector.merge(other.vvector));
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ORSet<?> other
                && elementsMap.equals(other.elementsMap)
                && vvector.equals(other.vvector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementsMap, vvector);
    }

    @Override
    public String toString() {
        return "ORSet(" + elementsMap.keySet() + ")";
    }
}
