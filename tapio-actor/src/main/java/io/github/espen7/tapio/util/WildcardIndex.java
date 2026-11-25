/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * 按 ActorPath 元素查找部署配置的索引，对应上游
 * {@code actor/src/main/scala/org/apache/pekko/util/WildcardIndex.scala}。
 *
 * <p>普通通配符 {@code *} 匹配单个路径元素，末尾的 {@code **} 匹配剩余全部元素。
 * 查找时先尝试单通配符树，找不到再尝试双通配符树。</p>
 *
 * @param <T> 索引保存的值类型
 */
public final class WildcardIndex<T> {
    private final WildcardTree<T> wildcardTree;
    private final WildcardTree<T> doubleWildcardTree;

    /** 创建一个空索引。 */
    public WildcardIndex() {
        this(new WildcardTree<>(), new WildcardTree<>());
    }

    private WildcardIndex(WildcardTree<T> wildcardTree, WildcardTree<T> doubleWildcardTree) {
        this.wildcardTree = wildcardTree;
        this.doubleWildcardTree = doubleWildcardTree;
    }

    /**
     * 插入一个路径及其值。
     *
     * @param elems 路径元素
     * @param d 与路径关联的值
     * @return 插入后的新索引
     * @throws IllegalArgumentException 当 {@code **} 作为元素后缀出现时
     */
    public WildcardIndex<T> insert(String[] elems, T d) {
        if (elems.length == 0) return this;
        String last = elems[elems.length - 1];
        if ("**".equals(last)) {
            return new WildcardIndex<>(wildcardTree, doubleWildcardTree.insert(iterator(elems), d));
        }
        return new WildcardIndex<>(wildcardTree.insert(iterator(elems), d), doubleWildcardTree);
    }

    /**
     * 查找路径对应的值。
     *
     * @param elems 路径元素
     * @return 匹配的值
     */
    public Optional<T> find(Iterable<String> elems) {
        WildcardTree<T> found;
        if (wildcardTree.isEmpty()) {
            found = doubleWildcardTree.isEmpty()
                    ? new WildcardTree<>()
                    : doubleWildcardTree.findWithTerminalDoubleWildcard(elems.iterator());
        } else {
            WildcardTree<T> withSingleWildcard = wildcardTree.findWithSingleWildcard(elems.iterator());
            found = withSingleWildcard.isEmpty()
                    ? doubleWildcardTree.findWithTerminalDoubleWildcard(elems.iterator())
                    : withSingleWildcard;
        }
        return found.data();
    }

    /**
     * @return 索引为空时返回 {@code true}
     */
    public boolean isEmpty() {
        return wildcardTree.isEmpty() && doubleWildcardTree.isEmpty();
    }

    private static Iterator<String> iterator(String[] elems) {
        return new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < elems.length;
            }

            @Override
            public String next() {
                return elems[index++];
            }
        };
    }

    /**
     * 单个通配符树节点，对应上游 {@code WildcardTree}。
     *
     * @param <T> 索引保存的值类型
     */
    static final class WildcardTree<T> {
        private final T data;
        private final Map<String, WildcardTree<T>> children;
        private final Map<String, WildcardTree<T>> wildcardSuffixChildren;

        WildcardTree() {
            this(null, Map.of(), Map.of());
        }

        private WildcardTree(T data, Map<String, WildcardTree<T>> children,
                             Map<String, WildcardTree<T>> wildcardSuffixChildren) {
            this.data = data;
            this.children = children;
            this.wildcardSuffixChildren = wildcardSuffixChildren;
        }

        Optional<T> data() {
            return Optional.ofNullable(data);
        }

        boolean isEmpty() {
            return data == null && children.isEmpty();
        }

        WildcardTree<T> insert(Iterator<String> elems, T d) {
            if (!elems.hasNext()) return new WildcardTree<>(d, children, wildcardSuffixChildren);
            String e = elems.next();
            if (!"**".equals(e) && e.endsWith("**")) {
                throw new IllegalArgumentException(
                        "double wildcard can't be used as a suffix (e.g. /user/actor**), "
                                + "only as a full subPath element (e.g. /user/actor/**)");
            } else if (!"*".equals(e) && !"**".equals(e) && e.endsWith("*")) {
                String base = e.substring(0, e.length() - 1);
                Map<String, WildcardTree<T>> updated = new HashMap<>(wildcardSuffixChildren);
                updated.put(base, wildcardSuffixChildren.getOrDefault(e, new WildcardTree<>()).insert(elems, d));
                return new WildcardTree<>(data, children, updated);
            }
            Map<String, WildcardTree<T>> updated = new HashMap<>(children);
            updated.put(e, children.getOrDefault(e, new WildcardTree<>()).insert(elems, d));
            return new WildcardTree<>(data, updated, wildcardSuffixChildren);
        }

        WildcardTree<T> findWithSingleWildcard(Iterator<String> elems) {
            WildcardTree<T> current = this;
            while (true) {
                if (!elems.hasNext()) return current;
                String nextElement = elems.next();
                WildcardTree<T> branch = current.children.get(nextElement);
                if (branch == null) branch = current.children.get("*");
                if (branch == null) {
                    WildcardTree<T> suffix = null;
                    for (Map.Entry<String, WildcardTree<T>> entry : current.wildcardSuffixChildren.entrySet()) {
                        if (nextElement.startsWith(entry.getKey())) {
                            suffix = entry.getValue();
                            break;
                        }
                    }
                    if (suffix == null) return new WildcardTree<>();
                    branch = suffix;
                }
                current = branch;
            }
        }

        WildcardTree<T> findWithTerminalDoubleWildcard(Iterator<String> elems) {
            return findWithTerminalDoubleWildcard(elems, new WildcardTree<>());
        }

        private WildcardTree<T> findWithTerminalDoubleWildcard(Iterator<String> elems, WildcardTree<T> alt) {
            WildcardTree<T> current = this;
            WildcardTree<T> currentAlt = alt;
            while (true) {
                if (!elems.hasNext()) return current;
                currentAlt = current.children.getOrDefault("**", currentAlt);
                String nextElement = elems.next();
                WildcardTree<T> branch = current.children.get(nextElement);
                if (branch == null) branch = current.children.get("*");
                if (branch == null) return currentAlt;
                current = branch;
            }
        }
    }
}
