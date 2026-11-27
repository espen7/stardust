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
package io.github.espen7.tapio.discovery.aggregate;

import io.github.espen7.tapio.actor.ExtendedActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.discovery.Discovery;
import io.github.espen7.tapio.discovery.Lookup;
import io.github.espen7.tapio.discovery.ServiceDiscovery;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * INTERNAL API：按顺序聚合多个 {@link ServiceDiscovery} 实现，返回首个非空结果。
 */
@InternalApi
public final class AggregateServiceDiscovery extends ServiceDiscovery {

    private final List<ServiceDiscovery> delegates;

    public AggregateServiceDiscovery(ExtendedActorSystem system) {
        List<String> methods = system.settings().config().getStringList("tapio.discovery.aggregate.discovery-methods");
        Discovery discovery = Discovery.get(system);
        this.delegates = methods.stream().map(discovery::loadServiceDiscovery).toList();
    }

    @Override
    public CompletionStage<Resolved> lookup(Lookup lookup, Duration resolveTimeout) {
        return lookupSequential(0, lookup, resolveTimeout);
    }

    private CompletionStage<Resolved> lookupSequential(int index, Lookup lookup, Duration resolveTimeout) {
        if (index >= delegates.size()) {
            return CompletableFuture.completedFuture(new Resolved(lookup.serviceName(), List.of()));
        }
        return delegates.get(index).lookup(lookup, resolveTimeout).thenCompose(resolved -> {
            if (!resolved.addresses().isEmpty()) {
                return CompletableFuture.completedFuture(resolved);
            }
            return lookupSequential(index + 1, lookup, resolveTimeout);
        });
    }
}
