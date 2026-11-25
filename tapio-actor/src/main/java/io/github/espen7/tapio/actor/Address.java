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
import java.util.Optional;

/**
 * ActorSystem 的地址。可以表达带主机和端口的地址，本地 Provider 只匹配本系统地址。
 *
 * @param protocol 地址协议
 * @param system ActorSystem 名称
 * @param host 主机，局部地址使用 {@code null}
 * @param port 端口，局部地址使用 {@code null}
 */
public record Address(String protocol, String system, String host, Integer port) implements Serializable {
    /**
     * 创建局部地址。
     *
     * @param protocol 地址协议
     * @param system ActorSystem 名称
     */
    public Address(String protocol, String system) {
        this(protocol, system, null, null);
    }

    /**
     * 返回可选的主机。
     *
     * @return 局部地址返回空值
     */
    public Optional<String> getHost() {
        return Optional.ofNullable(host);
    }

    /**
     * 返回可选的端口。
     *
     * @return 未指定端口时返回空值
     */
    public Optional<Integer> getPort() {
        return Optional.ofNullable(port);
    }

    @Override
    public String toString() {
        StringBuilder address = new StringBuilder(protocol).append("://").append(system);
        if (host != null) address.append('@').append(host);
        if (port != null) address.append(':').append(port);
        return address.toString();
    }
}
