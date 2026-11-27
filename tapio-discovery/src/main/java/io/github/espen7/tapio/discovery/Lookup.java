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
package io.github.espen7.tapio.discovery;

import java.util.Objects;
import java.util.Optional;

/**
 * 服务发现查询描述符，指定服务名与可选端口名、协议名。
 */
public record Lookup(String serviceName, Optional<String> portName, Optional<String> protocol) {

    public Lookup {
        Objects.requireNonNull(serviceName, "serviceName");
        portName = portName == null ? Optional.empty() : portName;
        protocol = protocol == null ? Optional.empty() : protocol;
    }

    public static Lookup create(String serviceName) {
        return new Lookup(serviceName, Optional.empty(), Optional.empty());
    }

    public Lookup withPortName(String portName) {
        return new Lookup(serviceName,
                (portName == null || portName.isEmpty()) ? Optional.empty() : Optional.of(portName),
                protocol);
    }

    public Lookup withProtocol(String protocol) {
        return new Lookup(serviceName,
                portName,
                (protocol == null || protocol.isEmpty()) ? Optional.empty() : Optional.of(protocol));
    }
}
