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

import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.actor.NoSerializationVerificationNeeded;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * 服务发现抽象基类，支持按 {@link Lookup} 异步解析服务端点列表。
 */
public abstract class ServiceDiscovery {

    /** 解析超时异常。 */
    public static final class DiscoveryTimeoutException extends RuntimeException {
        public DiscoveryTimeoutException(String reason) {
            super(reason);
        }
    }

    /** 单个已解析的服务目标端点。 */
    public record ResolvedTarget(String host, Optional<Integer> port, Optional<InetAddress> address) {
        public ResolvedTarget {
            Objects.requireNonNull(host, "host");
            port = port == null ? Optional.empty() : port;
            address = address == null ? Optional.empty() : address;
        }

        public static ResolvedTarget create(String host, Optional<Integer> port, Optional<InetAddress> address) {
            return new ResolvedTarget(host, port, address);
        }
    }

    /** 服务发现解析结果。 */
    public record Resolved(String serviceName, List<ResolvedTarget> addresses)
            implements DeadLetterSuppression, NoSerializationVerificationNeeded {
        public Resolved {
            Objects.requireNonNull(serviceName, "serviceName");
            addresses = addresses == null ? List.of() : List.copyOf(addresses);
        }

        public List<ResolvedTarget> getAddresses() {
            return addresses;
        }
    }

    /**
     * 按 {@link Lookup} 解析服务实例端点。
     *
     * @param lookup 查询条件
     * @param resolveTimeout 解析超时时间
     * @return 解析结果异步凭证
     */
    public abstract CompletionStage<Resolved> lookup(Lookup lookup, Duration resolveTimeout);

    /**
     * 按服务名解析服务实例端点。
     */
    public final CompletionStage<Resolved> lookup(String serviceName, Duration resolveTimeout) {
        return lookup(Lookup.create(serviceName), resolveTimeout);
    }
}
