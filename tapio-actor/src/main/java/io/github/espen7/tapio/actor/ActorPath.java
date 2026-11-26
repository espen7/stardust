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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Actor 的逻辑地址。路径相等只比较地址文本，ActorRef identity 还会比较 UID。
 */
public final class ActorPath implements Comparable<ActorPath>, Serializable {

    /** Tapio ActorPath 使用的 scheme，同时也是 runtime 配置命名空间。 */
    public static final String Scheme = "tapio";
    /** 未定义 Actor incarnation UID。 */
    public static final int UndefinedUid = 0;
    private static final String ValidSymbols = "-_.*$+:@&=,!~';";
    private final String value;
    private final int uid;
    private final ActorPath parent;
    private final Address address;

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
        this.address = null;
    }

    ActorPath(Address address) {
        this.value = address + "/";
        this.uid = UndefinedUid;
        this.parent = null;
        this.address = address;
    }

    /**
     * 在已有路径下创建未定义 UID 的子路径。
     *
     * @param parent 父级路径
     * @param name 子路径的最后一段
     */
    ActorPath(ActorPath parent, String name) {
        this(parent, name, UndefinedUid);
    }

    ActorPath(ActorPath parent, String name, int uid) {
        this.value = parent + (parent.isRoot() ? "" : "/") + name;
        this.uid = uid;
        this.parent = parent;
        this.address = parent.address;
    }

    private boolean isRoot() {
        return parent == null && value.endsWith("/") && toStringWithoutAddress().equals("/");
    }

    /**
     * 返回构造时关联的父级路径。
     *
     * @return 根路径返回自身，运行时子路径返回其父级，其他直接从文本构造的路径返回 {@code null}
     */
    ActorPath parent() {
        return isRoot() ? this : parent;
    }

    /**
     * 返回此路径的 ActorSystem 地址。
     *
     * @return 路径中的地址
     * @throws IllegalArgumentException 路径文本缺少有效地址
     */
    public Address address() {
        if (address != null) return address;
        try {
            Address parsed = address(new URI(value));
            if (parsed != null) return parsed;
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("cannot parse as ActorPath: " + value, error);
        }
        throw new IllegalArgumentException("cannot parse as ActorPath: " + value);
    }

    static Address address(URI uri) {
        String protocol = uri.getScheme();
        String authority = uri.getRawAuthority();
        if (protocol != null && authority != null) {
            if (uri.getUserInfo() == null && uri.getHost() != null && uri.getPort() == -1) {
                return new Address(protocol, uri.getHost());
            }
            if (uri.getUserInfo() != null && uri.getHost() != null && uri.getPort() != -1) {
                return new Address(protocol, uri.getUserInfo(), uri.getHost(), uri.getPort());
            }
        }
        return null;
    }

    List<String> elements() {
        return Arrays.asList(toStringWithoutAddress().substring(1).split("/", -1));
    }

    static void validatePathElement(String element) {
        for (int index = 0; index < element.length(); index++) {
            char character = element.charAt(index);
            if (index == 0 && character == '$') {
                throw invalidPathElement(element, index);
            }
            if (character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
                    || character >= '0' && character <= '9' || ValidSymbols.indexOf(character) >= 0) {
                continue;
            }
            if (character == '%' && index + 2 < element.length()
                    && isHexDigit(element.charAt(index + 1)) && isHexDigit(element.charAt(index + 2))) {
                index += 2;
            } else {
                throw invalidPathElement(element, index);
            }
        }
    }

    private static boolean isHexDigit(char character) {
        return character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
                || character >= 'A' && character <= 'F';
    }

    private static InvalidActorNameException invalidPathElement(String element, int index) {
        return new InvalidActorNameException("Invalid actor path element [" + element + "], illegal character ["
                + element.charAt(index) + "] at position: " + index + ". Actor paths MUST: not start with `$`, "
                + "include only ASCII letters and can only contain these special characters: " + ValidSymbols + ".");
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
        if (isRoot()) return "/";
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
        List<String> left = isRoot() ? List.of() : elements();
        List<String> right = other.isRoot() ? List.of() : other.elements();
        int leftIndex = left.size() - 1;
        int rightIndex = right.size() - 1;
        while (leftIndex >= 0 && rightIndex >= 0) {
            int order = left.get(leftIndex--).compareTo(right.get(rightIndex--));
            if (order != 0) return order;
        }
        if (leftIndex != rightIndex) return Integer.compare(rightIndex, leftIndex);
        return value.substring(0, value.length() - toStringWithoutAddress().length())
                .compareTo(other.value.substring(0, other.value.length() - other.toStringWithoutAddress().length()));
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
