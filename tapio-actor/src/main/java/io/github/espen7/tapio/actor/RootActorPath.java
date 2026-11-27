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
import java.util.List;
import java.util.Objects;

/**
 * ActorPath 层级的根节点。每个 ActorSystem 对应一个根路径。
 */
public final class RootActorPath implements ActorPath {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final List<String> EMPTY_ELEMENTS = List.of();

    private final Address address;
    private final String name;
    private final String stringValue;

    /**
     * 使用默认根名称 {@code "/"} 创建根路径。
     *
     * @param address 系统地址
     */
    public RootActorPath(Address address) {
        this(address, "/");
    }

    /**
     * 创建指定名称的根路径。
     *
     * @param address 系统地址
     * @param name    根名称，通常为 {@code "/"}
     */
    public RootActorPath(Address address, String name) {
        this.address = Objects.requireNonNull(address, "address must not be null");
        this.name = Objects.requireNonNull(name, "name must not be null");
        if (!(name.length() == 1 || name.indexOf('/', 1) == -1)) {
            throw new IllegalArgumentException(
                    "/ may only exist at the beginning of the root actors name, "
                            + "it is a path separator and is not legal in ActorPath names: [" + name + "]");
        }
        if (name.indexOf('#') != -1) {
            throw new IllegalArgumentException(
                    "# is a fragment separator and is not legal in ActorPath names: [" + name + "]");
        }
        this.stringValue = address + name;
    }

    @Override
    public Address address() {
        return address;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ActorPath parent() {
        return this;
    }

    @Override
    public RootActorPath root() {
        return this;
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
        return EMPTY_ELEMENTS;
    }

    @Override
    public String toStringWithoutAddress() {
        return "/";
    }

    @Override
    public String toString() {
        return stringValue;
    }

    @Override
    public String toSerializationFormat() {
        return stringValue;
    }

    @Override
    public String toStringWithAddress(Address addr) {
        return address.host() != null ? address + name : addr + name;
    }

    @Override
    public String toSerializationFormatWithAddress(Address addr) {
        return toStringWithAddress(addr);
    }

    @Override
    public int compareTo(ActorPath other) {
        if (other instanceof RootActorPath rootOther) {
            return toString().compareTo(rootOther.toString());
        }
        return 1;
    }

    @Override
    @InternalApi
    public int uid() {
        return UndefinedUid;
    }

    @Override
    @InternalApi
    public ActorPath withUid(int uid) {
        if (uid == UndefinedUid) {
            return this;
        }
        throw new IllegalStateException("RootActorPath must have undefinedUid, [" + uid + " != " + UndefinedUid + "]");
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RootActorPath that
                && address.equals(that.address)
                && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, name);
    }
}
