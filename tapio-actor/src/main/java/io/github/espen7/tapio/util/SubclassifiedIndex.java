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

import io.github.espen7.tapio.annotation.InternalApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 按子类型组织 channel，并返回每次修改后需要更新的订阅缓存项。 */
@InternalApi
public class SubclassifiedIndex<K, V> {
    protected Set<V> values;
    protected List<Nonroot<K, V>> subkeys = List.of();
    protected final SubclassifiedIndex<K, V> root;
    protected final Subclassification<K> sc;

    public SubclassifiedIndex(Subclassification<K> sc) {
        this(null, Set.of(), sc);
    }

    private SubclassifiedIndex(SubclassifiedIndex<K, V> root, Set<V> values, Subclassification<K> sc) {
        this.root = root == null ? this : root;
        this.values = values;
        this.sc = sc;
    }

    /** 新 channel 继承已登记的父 channel 的订阅。 */
    public Map<K, Set<V>> addKey(K key) {
        return mergeChangesByKey(innerAddKey(key));
    }

    protected List<Map.Entry<K, Set<V>>> innerAddKey(K key) {
        boolean found = false;
        List<Map.Entry<K, Set<V>>> changes = new ArrayList<>();
        for (Nonroot<K, V> node : subkeys) {
            if (sc.isEqual(key, node.key)) {
                found = true;
            } else if (sc.isSubclass(key, node.key)) {
                found = true;
                changes.addAll(node.innerAddKey(key));
            }
        }
        if (!found) {
            changes.addAll(integrate(new Nonroot<>(root, key, values)));
            changes.add(Map.entry(key, values));
        }
        return changes;
    }

    /** 添加对指定 channel 以及其子 channel 的订阅。 */
    public Map<K, Set<V>> addValue(K key, V value) {
        return mergeChangesByKey(innerAddValue(key, value));
    }

    protected List<Map.Entry<K, Set<V>>> innerAddValue(K key, V value) {
        boolean found = false;
        List<Map.Entry<K, Set<V>>> changes = new ArrayList<>();
        for (Nonroot<K, V> node : subkeys) {
            if (sc.isSubclass(key, node.key)) {
                found = true;
                changes.addAll(node.innerAddValue(key, value));
            }
        }
        if (!found) {
            Set<V> added = new HashSet<>(values);
            added.add(value);
            Set<V> updated = Set.copyOf(added);
            Nonroot<K, V> node = new Nonroot<>(root, key, updated);
            changes.addAll(integrate(node));
            changes.addAll(node.innerAddValue(key, value));
            changes.add(Map.entry(key, updated));
        }
        return changes;
    }

    /** 移除指定 channel 及其子 channel 的订阅，返回变更 channel 的完整订阅。 */
    public Map<K, Set<V>> removeValue(K key, V value) {
        Map<K, Set<V>> changes = new HashMap<>();
        for (K changed : mergeChangesByKey(innerRemoveValue(key, value)).keySet()) {
            changes.put(changed, findValues(changed));
        }
        return changes;
    }

    protected List<Map.Entry<K, Set<V>>> innerRemoveValue(K key, V value) {
        boolean found = false;
        List<Map.Entry<K, Set<V>>> changes = new ArrayList<>();
        for (Nonroot<K, V> node : subkeys) {
            if (sc.isSubclass(key, node.key)) {
                found = true;
                changes.addAll(node.innerRemoveValue(key, value));
            }
        }
        if (!found) {
            Nonroot<K, V> node = new Nonroot<>(root, key, values);
            changes.addAll(integrate(node));
            changes.addAll(node.innerRemoveValue(key, value));
        }
        return changes;
    }

    /** 移除该订阅者的全部订阅并返回差异。 */
    public Map<K, Set<V>> removeValue(V value) {
        List<Map.Entry<K, Set<V>>> changes = new ArrayList<>();
        for (Nonroot<K, V> node : subkeys) changes.addAll(node.innerRemoveValue(node.key, value));
        return mergeChangesByKey(changes);
    }

    protected final Set<V> findValues(K key) {
        return root.innerFindValues(key);
    }

    protected Set<V> innerFindValues(K key) {
        Set<V> found = new HashSet<>();
        for (Nonroot<K, V> node : subkeys) {
            if (sc.isSubclass(key, node.key)) found.addAll(node.innerFindValues(key));
        }
        return Set.copyOf(found);
    }

    protected final Set<K> findSubKeysExcept(K key, List<Nonroot<K, V>> except) {
        return root.innerFindSubKeys(key, except);
    }

    protected Set<K> innerFindSubKeys(K key, List<Nonroot<K, V>> except) {
        Set<K> found = new HashSet<>();
        for (Nonroot<K, V> node : subkeys) {
            if (sc.isEqual(key, node.key)) continue;
            found.addAll(node.innerFindSubKeys(key, except));
            if (sc.isSubclass(node.key, key) && except.stream().noneMatch(existing -> sc.isEqual(key, existing.key))) {
                found.add(node.key);
            }
        }
        return found;
    }

    private List<Map.Entry<K, Set<V>>> integrate(Nonroot<K, V> node) {
        List<Nonroot<K, V>> subsub = new ArrayList<>();
        List<Nonroot<K, V>> sub = new ArrayList<>();
        for (Nonroot<K, V> child : subkeys) {
            if (sc.isSubclass(child.key, node.key)) subsub.add(child);
            else sub.add(child);
        }
        sub.add(node);
        subkeys = List.copyOf(sub);
        List<Nonroot<K, V>> updated = new ArrayList<>(subsub.isEmpty() ? node.subkeys : subsub);
        for (K key : findSubKeysExcept(node.key, updated)) updated.add(new Nonroot<>(root, key, values));
        node.subkeys = List.copyOf(updated);
        return node.subkeys.stream().map(child -> Map.entry(child.key, child.values)).toList();
    }

    private Map<K, Set<V>> mergeChangesByKey(List<Map.Entry<K, Set<V>>> changes) {
        Map<K, Set<V>> merged = new HashMap<>();
        for (Map.Entry<K, Set<V>> change : changes) {
            merged.computeIfAbsent(change.getKey(), key -> new HashSet<>()).addAll(change.getValue());
        }
        merged.replaceAll((key, value) -> Set.copyOf(value));
        return merged;
    }

    protected static class Nonroot<K, V> extends SubclassifiedIndex<K, V> {
        final K key;

        Nonroot(SubclassifiedIndex<K, V> root, K key, Set<V> values) {
            super(root, values, root.sc);
            this.key = key;
        }

        @Override
        protected List<Map.Entry<K, Set<V>>> innerAddValue(K key, V value) {
            return sc.isEqual(key, this.key) ? addValue(value) : super.innerAddValue(key, value);
        }

        private List<Map.Entry<K, Set<V>>> addValue(V value) {
            List<Map.Entry<K, Set<V>>> changes = new ArrayList<>();
            for (Nonroot<K, V> node : subkeys) changes.addAll(node.addValue(value));
            if (!values.contains(value)) {
                Set<V> updated = new HashSet<>(values);
                updated.add(value);
                values = Set.copyOf(updated);
                changes.add(Map.entry(key, Set.of(value)));
            }
            return changes;
        }

        @Override
        protected List<Map.Entry<K, Set<V>>> innerRemoveValue(K key, V value) {
            return sc.isEqual(key, this.key) ? removeValue(value, new ArrayList<>()) : super.innerRemoveValue(key, value);
        }

        private List<Map.Entry<K, Set<V>>> removeValue(V value, List<Map.Entry<K, Set<V>>> changes) {
            for (Nonroot<K, V> node : subkeys) node.removeValue(value, changes);
            if (values.contains(value)) {
                Set<V> updated = new HashSet<>(values);
                updated.remove(value);
                values = Set.copyOf(updated);
                changes.add(Map.entry(key, Set.of(value)));
            }
            return changes;
        }

        @Override
        protected Set<V> innerFindValues(K key) {
            return sc.isEqual(key, this.key) ? values : super.innerFindValues(key);
        }
    }
}
