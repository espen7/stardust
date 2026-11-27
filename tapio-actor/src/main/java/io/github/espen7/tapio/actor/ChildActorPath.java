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

import io.github.espen7.tapio.annotation.InternalApi;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link ActorPath} 的子路径节点。
 */
public final class ChildActorPath implements ActorPath {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final int VISIBLE_MAGIC = 0x971E137B;
    private static final int HIDDEN_MAGIC_A = 0x95543787;
    private static final int HIDDEN_MAGIC_B = 0x2AD7EB25;
    private static final int VISIBLE_MIXER = 0x52DCE729;
    private static final int HIDDEN_MIXER_A = 0x7B7D159C;
    private static final int HIDDEN_MIXER_B = 0x6BCE6396;
    private static final int FINAL_MIXER_1 = 0x85EBCA6B;
    private static final int FINAL_MIXER_2 = 0xC2B2AE35;
    private static final int SEED_STRING = 0xF7CA7FD2;

    private final ActorPath parent;
    private final String name;
    private final int uid;
    private final int toStringOffset;

    /**
     * INTERNAL API：构造子路径节点。
     *
     * @param parent 父级路径
     * @param name   当前节点名称
     * @param uid    Actor incarnation UID
     */
    @InternalApi
    public ChildActorPath(ActorPath parent, String name, int uid) {
        this.parent = Objects.requireNonNull(parent, "parent must not be null");
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.uid = uid;
        if (name.indexOf('/') != -1) {
            throw new IllegalArgumentException(
                    "/ is a path separator and is not legal in ActorPath names: [" + name + "]");
        }
        if (name.indexOf('#') != -1) {
            throw new IllegalArgumentException(
                    "# is a fragment separator and is not legal in ActorPath names: [" + name + "]");
        }
        this.toStringOffset = switch (parent) {
            case RootActorPath r -> r.address().toString().length() + r.name().length();
            case ChildActorPath c -> c.toStringLength() + 1;
        };
    }

    @Override
    public ActorPath parent() {
        return parent;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    @InternalApi
    public int uid() {
        return uid;
    }

    @Override
    public Address address() {
        return root().address();
    }

    @Override
    public ActorPath child(String child) {
        int separator = child.indexOf('#');
        if (separator < 0) {
            return new ChildActorPath(this, child, UndefinedUid);
        }
        return new ChildActorPath(this, child.substring(0, separator),
                Integer.parseInt(child.substring(separator + 1)));
    }

    @Override
    public List<String> elements() {
        List<String> acc = new ArrayList<>();
        ActorPath current = this;
        while (!(current instanceof RootActorPath)) {
            acc.add(current.name());
            current = current.parent();
        }
        Collections.reverse(acc);
        return List.copyOf(acc);
    }

    @Override
    public RootActorPath root() {
        ActorPath current = this;
        while (!(current instanceof RootActorPath rootPath)) {
            current = current.parent();
        }
        return rootPath;
    }

    @Override
    @InternalApi
    public ActorPath withUid(int uid) {
        return uid == this.uid ? this : new ChildActorPath(parent, name, uid);
    }

    private int toStringLength() {
        return toStringOffset + name.length();
    }

    @Override
    public String toString() {
        int length = toStringLength();
        return buildToString(new StringBuilder(length), length, 0, RootActorPath::toString).toString();
    }

    @Override
    public String toSerializationFormat() {
        int length = toStringLength();
        StringBuilder sb = buildToString(new StringBuilder(length + 12), length, 0, RootActorPath::toString);
        return appendUidFragment(sb).toString();
    }

    @Override
    public String toStringWithAddress(Address addr) {
        if (IgnoreActorRef.isIgnoreRefPath(this)) {
            return toString();
        }
        int diff = addressStringLengthDiff(addr);
        int length = toStringLength() + diff;
        return buildToString(new StringBuilder(length), length, diff, r -> r.toStringWithAddress(addr)).toString();
    }

    @Override
    public String toSerializationFormatWithAddress(Address addr) {
        if (IgnoreActorRef.isIgnoreRefPath(this)) {
            return toString();
        }
        int diff = addressStringLengthDiff(addr);
        int length = toStringLength() + diff;
        StringBuilder sb = buildToString(new StringBuilder(length + 12), length, diff, r -> r.toStringWithAddress(addr));
        return appendUidFragment(sb).toString();
    }

    private int addressStringLengthDiff(Address address) {
        RootActorPath r = root();
        return r.address().host() != null ? 0 : address.toString().length() - r.address().toString().length();
    }

    private StringBuilder buildToString(StringBuilder sb, int length, int diff,
                                        java.util.function.Function<RootActorPath, String> rootString) {
        sb.setLength(length);
        ActorPath p = this;
        for (;;) {
            switch (p) {
                case RootActorPath r -> {
                    String rootStr = rootString.apply(r);
                    sb.replace(0, rootStr.length(), rootStr);
                    return sb;
                }
                case ChildActorPath c -> {
                    int start = c.toStringOffset + diff;
                    int end = start + c.name.length();
                    sb.replace(start, end, c.name);
                    if (c != this) {
                        sb.replace(end, end + 1, "/");
                    }
                    p = c.parent;
                }
            }
        }
    }

    private StringBuilder appendUidFragment(StringBuilder sb) {
        return uid == UndefinedUid ? sb : sb.append('#').append(uid);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ActorPath that)) {
            return false;
        }
        ActorPath left = this;
        ActorPath right = that;
        for (;;) {
            if (left == right) return true;
            if (left instanceof RootActorPath) return left.equals(right);
            if (right instanceof RootActorPath) return right.equals(left);
            if (!left.name().equals(right.name())) return false;
            left = left.parent();
            right = right.parent();
        }
    }

    @Override
    public int hashCode() {
        int h = 42 ^ VISIBLE_MAGIC;
        int c = HIDDEN_MAGIC_A;
        int k = HIDDEN_MAGIC_B;
        ActorPath p = this;
        for (;;) {
            if (p instanceof RootActorPath r) {
                h = extendHash(h, r.hashCode(), c, k);
                return finalizeHash(h);
            }
            h = extendHash(h, stringHash(p.name()), c, k);
            c = c * 5 + HIDDEN_MIXER_A;
            k = k * 5 + HIDDEN_MIXER_B;
            p = p.parent();
        }
    }

    @Override
    public int compareTo(ActorPath other) {
        ActorPath left = this;
        ActorPath right = other;
        for (;;) {
            if (left == right) return 0;
            if (left instanceof RootActorPath) return left.compareTo(right);
            if (right instanceof RootActorPath) return -right.compareTo(left);
            int cmp = left.name().compareTo(right.name());
            if (cmp != 0) return cmp;
            left = left.parent();
            right = right.parent();
        }
    }

    private static int extendHash(int hash, int value, int magicA, int magicB) {
        return (hash ^ Integer.rotateLeft(value * magicA, 11) * magicB) * 3 + VISIBLE_MIXER;
    }

    private static int finalizeHash(int hash) {
        int i = hash ^ (hash >>> 16);
        i *= FINAL_MIXER_1;
        i ^= (i >>> 13);
        i *= FINAL_MIXER_2;
        i ^= (i >>> 16);
        return i;
    }

    private static int stringHash(String s) {
        int h = (s.length() * SEED_STRING) ^ VISIBLE_MAGIC;
        int c = HIDDEN_MAGIC_A;
        int k = HIDDEN_MAGIC_B;
        int j = 0;
        while (j + 1 < s.length()) {
            int i = (s.charAt(j) << 16) + s.charAt(j + 1);
            h = extendHash(h, i, c, k);
            c = c * 5 + HIDDEN_MIXER_A;
            k = k * 5 + HIDDEN_MIXER_B;
            j += 2;
        }
        if (j < s.length()) {
            h = extendHash(h, s.charAt(j), c, k);
        }
        return finalizeHash(h);
    }
}
