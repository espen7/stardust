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
package io.github.espen7.tapio.discovery.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.discovery.ServiceDiscovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * INTERNAL API：解析 {@code tapio.discovery.config.services} 静态服务表。
 */
@InternalApi
final class ConfigServicesParser {
    private ConfigServicesParser() {
    }

    static Map<String, ServiceDiscovery.Resolved> parse(Config config) {
        Map<String, ServiceDiscovery.Resolved> result = new HashMap<>();
        for (var entry : config.root().entrySet()) {
            String serviceName = entry.getKey();
            Config serviceCfg = config.getConfig(ConfigUtil.quoteString(serviceName));
            List<ServiceDiscovery.ResolvedTarget> targets = new ArrayList<>();
            for (Config ep : serviceCfg.getConfigList("endpoints")) {
                String host = ep.getString("host");
                Optional<Integer> port = ep.hasPath("port") ? Optional.of(ep.getInt("port")) : Optional.empty();
                targets.add(ServiceDiscovery.ResolvedTarget.create(host, port, Optional.empty()));
            }
            result.put(serviceName, new ServiceDiscovery.Resolved(serviceName, targets));
        }
        return Map.copyOf(result);
    }
}
