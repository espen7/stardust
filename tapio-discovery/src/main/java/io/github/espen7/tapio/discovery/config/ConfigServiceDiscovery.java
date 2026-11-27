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

import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.discovery.Lookup;
import io.github.espen7.tapio.discovery.ServiceDiscovery;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * INTERNAL API：基于 HOCON 配置的本地/静态服务发现实现（{@code tapio.discovery.method = "config"}）。
 */
@InternalApi
public final class ConfigServiceDiscovery extends ServiceDiscovery {

    private final Map<String, Resolved> resolvedServices;

    public ConfigServiceDiscovery(ExtendedActorSystem system) {
        LoggingAdapter log = Logging.getLogger(system, this, getClass());
        String servicesPath = system.settings().config().getString("tapio.discovery.config.services-path");
        this.resolvedServices = ConfigServicesParser.parse(system.settings().config().getConfig(servicesPath));
        log.debug("Config discovery serving: {}", resolvedServices);
    }

    @Override
    public CompletionStage<Resolved> lookup(Lookup lookup, Duration resolveTimeout) {
        Resolved resolved = resolvedServices.getOrDefault(
                lookup.serviceName(), new Resolved(lookup.serviceName(), List.of()));
        return CompletableFuture.completedFuture(resolved);
    }
}
