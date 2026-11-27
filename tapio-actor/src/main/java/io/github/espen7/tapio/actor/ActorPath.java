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

import io.github.espen7.tapio.annotation.DoNotInherit;
import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Actor 的逻辑地址。路径相等只比较地址和路径元素，ActorRef identity 还会比较 UID。
 */
@DoNotInherit
public sealed interface ActorPath extends Comparable<ActorPath>, Serializable permits RootActorPath, ChildActorPath {

    /** Tapio ActorPath 使用的 scheme，同时也是 runtime 配置命名空间。 */
    String Scheme = "tapio";
    /** 未定义 Actor incarnation UID。 */
    int UndefinedUid = 0;

    /**
     * 返回此路径所属的 ActorSystem 地址。
     *
     * @return 路径中的地址
     */
    Address address();

    /**
     * 返回此路径指向的 Actor 名称。
     *
     * @return 最后一个路径元素；根路径为 {@code "/"}
     */
    String name();

    /**
     * 返回父级路径。
     *
     * @return 根路径返回自身，子路径返回其父级
     */
    ActorPath parent();

    /**
     * 在当前路径下创建子路径（支持 {@code name#uid} 片段以及多级 {@code a/b} 路径）。
     *
     * @param child 子路径名称或相对路径
     * @return 新的子路径
     */
    ActorPath child(String child);

    /**
     * 递归追加多个子路径名称。
     *
     * @param names 子路径名称序列
     * @return 目标后代路径
     */
    default ActorPath descendant(Iterable<String> names) {
        ActorPath path = this;
        for (String elem : names) {
            if (elem != null && !elem.isEmpty()) {
                path = path.child(elem);
            }
        }
        return path;
    }

    /**
     * 返回从根路径到当前路径的名称序列。
     *
     * @return 路径元素列表
     */
    List<String> elements();

    /**
     * 向上遍历并返回根路径 {@link RootActorPath}。
     *
     * @return 根路径
     */
    RootActorPath root();

    /**
     * 返回不包含 scheme 和 address 的路径文本，例如 {@code /user/a/b}。
     *
     * @return 不含地址的路径文本
     */
    default String toStringWithoutAddress() {
        List<String> elems = elements();
        return elems.isEmpty() ? "/" : "/" + String.join("/", elems);
    }

    /**
     * 生成将本地无 host/port 地址替换为指定地址的字符串表示。
     *
     * @param address 目标地址
     * @return 替换后的路径字符串
     */
    String toStringWithAddress(Address address);

    /**
     * 生成包含 UID 片段（若已定义）的序列化格式字符串。
     *
     * @return 序列化格式路径
     */
    String toSerializationFormat();

    /**
     * 生成包含 UID 片段并按需替换地址的序列化格式字符串。
     *
     * @param address 目标地址
     * @return 替换地址后的序列化格式路径
     */
    String toSerializationFormatWithAddress(Address address);

    /**
     * INTERNAL API：返回 Actor incarnation UID。
     *
     * @return UID，未定义时为 {@link #UndefinedUid}
     */
    @InternalApi
    int uid();

    /**
     * INTERNAL API：返回具有相同路径元素和指定 {@code uid} 的 ActorPath。
     *
     * @param uid 新的 UID
     * @return 带有指定 UID 的路径
     */
    @InternalApi
    ActorPath withUid(int uid);

    /**
     * 将字符串解析为 {@link ActorPath}。
     *
     * @param s 路径字符串
     * @return 解析后的 ActorPath
     * @throws MalformedURLException 当字符串无法解析为合法 ActorPath 时
     */
    static ActorPath fromString(String s) throws MalformedURLException {
        if (s == null) {
            throw new MalformedURLException("cannot parse as ActorPath: null");
        }
        try {
            URI uri = new URI(s);
            Address addr = address(uri);
            if (addr == null) {
                throw new MalformedURLException("cannot parse as ActorPath: " + s);
            }
            String rawPath = uri.getRawPath();
            String fragment = uri.getRawFragment();
            if (rawPath == null || rawPath.isEmpty() || "/".equals(rawPath)) {
                return new RootActorPath(addr);
            }
            String relative = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
            if (fragment != null && !fragment.isEmpty()) {
                relative = relative + "#" + fragment;
            }
            ActorPath current = new RootActorPath(addr);
            String[] parts = relative.split("/", -1);
            for (int i = 0; i < parts.length; i++) {
                String part = parts[i];
                if (part.isEmpty()) {
                    throw new MalformedURLException("cannot parse as ActorPath: " + s);
                }
                current = current.child(part);
            }
            return current;
        } catch (URISyntaxException | IllegalArgumentException error) {
            MalformedURLException malformed = new MalformedURLException("cannot parse as ActorPath: " + s);
            malformed.initCause(error);
            throw malformed;
        }
    }

    /**
     * 判断给定字符串是否为合法的 ActorPath 路径元素。
     *
     * @param s 路径元素
     * @return 合法返回 {@code true}
     */
    static boolean isValidPathElement(String s) {
        return findInvalidPathElementCharPosition(s) == PathValidation.VALID_PATH_CODE;
    }

    /**
     * 校验给定路径元素，不合法时抛出 {@link InvalidActorNameException}。
     *
     * @param element 路径元素
     */
    static void validatePathElement(String element) {
        validatePathElement(element, null);
    }

    /**
     * 校验给定路径元素，并在错误消息中附带可选完整路径。
     *
     * @param element 路径元素
     * @param fullPath 可选完整路径
     */
    static void validatePathElement(String element, String fullPath) {
        int pos = findInvalidPathElementCharPosition(element);
        if (pos == PathValidation.VALID_PATH_CODE) {
            return;
        }
        String fullPathMsg = fullPath != null ? " (in path [" + fullPath + "])" : "";
        if (pos == PathValidation.EMPTY_PATH_CODE) {
            throw new InvalidActorNameException("Actor path element must not be empty" + fullPathMsg);
        }
        throw new InvalidActorNameException("Invalid actor path element [" + element + "], illegal character ["
                + element.charAt(pos) + "] at position: " + pos + ". Actor paths MUST: not start with `$`, "
                + "include only ASCII letters and can only contain these special characters: "
                + PathValidation.VALID_SYMBOLS + "." + fullPathMsg);
    }

    private static int findInvalidPathElementCharPosition(String s) {
        if (s == null || s.isEmpty()) {
            return PathValidation.EMPTY_PATH_CODE;
        }
        byte[] bytes = s.getBytes(StandardCharsets.ISO_8859_1);
        int len = bytes.length;
        if (bytes[0] == '$') {
            return 0;
        }
        for (int pos = 0; pos < len; ) {
            byte b = bytes[pos];
            int flags = b >= 0 ? PathValidation.PATH_CHAR_FLAGS[b] : 0;
            if ((flags & PathValidation.VALID_CHAR_FLAG) != 0) {
                pos++;
            } else if (b == '%' && pos + 2 < len
                    && bytes[pos + 1] >= 0 && (PathValidation.PATH_CHAR_FLAGS[bytes[pos + 1]] & PathValidation.HEX_CHAR_FLAG) != 0
                    && bytes[pos + 2] >= 0 && (PathValidation.PATH_CHAR_FLAGS[bytes[pos + 2]] & PathValidation.HEX_CHAR_FLAG) != 0) {
                pos += 3;
            } else {
                return pos;
            }
        }
        return PathValidation.VALID_PATH_CODE;
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

    /** 路径字符校验常量与查找表。 */
    final class PathValidation {
        static final String VALID_SYMBOLS = "-_.*$+:@&=,!~';";
        static final int VALID_PATH_CODE = -1;
        static final int EMPTY_PATH_CODE = -2;
        static final byte VALID_CHAR_FLAG = 1;
        static final byte HEX_CHAR_FLAG = 2;
        static final byte[] PATH_CHAR_FLAGS = new byte[128];

        static {
            for (char c = 'a'; c <= 'z'; c++) PATH_CHAR_FLAGS[c] |= VALID_CHAR_FLAG;
            for (char c = 'A'; c <= 'Z'; c++) PATH_CHAR_FLAGS[c] |= VALID_CHAR_FLAG;
            for (char c = '0'; c <= '9'; c++) PATH_CHAR_FLAGS[c] |= VALID_CHAR_FLAG;
            for (int i = 0; i < VALID_SYMBOLS.length(); i++) {
                PATH_CHAR_FLAGS[VALID_SYMBOLS.charAt(i)] |= VALID_CHAR_FLAG;
            }
            for (char c = 'a'; c <= 'f'; c++) PATH_CHAR_FLAGS[c] |= HEX_CHAR_FLAG;
            for (char c = 'A'; c <= 'F'; c++) PATH_CHAR_FLAGS[c] |= HEX_CHAR_FLAG;
            for (char c = '0'; c <= '9'; c++) PATH_CHAR_FLAGS[c] |= HEX_CHAR_FLAG;
        }

        private PathValidation() {
        }
    }
}
