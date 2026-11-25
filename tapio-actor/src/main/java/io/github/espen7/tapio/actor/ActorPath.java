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
package io.github.espen7.tapio.actor;

import java.io.Serializable;

/**
 * Actor 的逻辑地址。路径相等只比较地址文本，ActorRef identity 还会比较 UID。
 */
public final class ActorPath implements Comparable<ActorPath>, Serializable {

    /** Tapio ActorPath 使用的 scheme，同时也是 runtime 配置命名空间。 */
    public static final String Scheme = "tapio";
    /** 未定义 Actor incarnation UID。 */
    public static final int UndefinedUid = 0;
    private final String value;
    private final int uid;
    private final ActorPath parent;

    /**
     * 从文本创建不带 UID 的 ActorPath。
     *
     * @param value 路径文本
     */
    public ActorPath(String value) {
        this(value, UndefinedUid);
    }

    /**
     * 从文本和 UID 创建 ActorPath。
     *
     * @param value 路径文本
     * @param uid Actor incarnation UID
     */
    public ActorPath(String value, int uid) {
        this.value = value;
        this.uid = uid;
        this.parent = null;
    }

    /**
     * 在已有路径下创建未定义 UID 的子路径。
     *
     * @param parent 父级路径
     * @param name 子路径的最后一段
     */
    ActorPath(ActorPath parent, String name) {
        this.value = parent + "/" + name;
        this.uid = UndefinedUid;
        this.parent = parent;
    }

    /**
     * 返回构造时关联的父级路径。
     *
     * @return 关联的父级路径，直接从文本构造时为 {@code null}
     */
    ActorPath parent() {
        return parent;
    }

    /**
     * 返回 Actor incarnation UID。
     *
     * @return UID，未定义时为 {@link #UndefinedUid}
     */
    public int uid() {
        return uid;
    }

    /**
     * 返回路径最后一个元素，对应 Pekko ActorPath.name。
     *
     * @return 最后一个路径元素
     */
    public String name() {
        int separator = value.lastIndexOf('/');
        return value.substring(separator + 1);
    }

    /**
     * 返回不包含 scheme 和 address 的路径文本。
     *
     * @return path elements 文本
     */
    public String toStringWithoutAddress() {
        int scheme = value.indexOf("://");
        if (scheme < 0) return value;
        int firstElement = value.indexOf('/', scheme + 3);
        return firstElement < 0 ? "/" : value.substring(firstElement);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int compareTo(ActorPath other) {
        return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ActorPath path && value.equals(path.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
