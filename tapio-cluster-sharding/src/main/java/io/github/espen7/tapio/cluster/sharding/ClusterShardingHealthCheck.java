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
package io.github.espen7.tapio.cluster.sharding;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.MemberStatus;
import io.github.espen7.tapio.pattern.Patterns;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 集群分片健康检查，验证已配置的 {@link ShardRegion} 是否已成功向协调器注册。
 */
public final class ClusterShardingHealthCheck implements Supplier<CompletionStage<Boolean>> {

    private final ActorSystem system;
    private final ClusterShardingHealthCheckSettings settings;
    private final Function<String, ActorRef> shardRegionProvider;

    private volatile boolean registered = false;
    private volatile long startedTimestamp = 0L;

    public ClusterShardingHealthCheck(ActorSystem system) {
        this(
                system,
                ClusterShardingHealthCheckSettings.create(
                        system.settings().config().getConfig("tapio.cluster.sharding.healthcheck")),
                name -> ClusterSharding.get(system).shardRegion(name));
    }

    ClusterShardingHealthCheck(
            ActorSystem system,
            ClusterShardingHealthCheckSettings settings,
            Function<String, ActorRef> shardRegionProvider) {
        this.system = system;
        this.settings = settings;
        this.shardRegionProvider = shardRegionProvider;
    }

    @Override
    public CompletionStage<Boolean> get() {
        if (registered || settings.names().isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        MemberStatus status = Cluster.get(system).selfMember().status();
        if (status == MemberStatus.Joining || status == MemberStatus.Removed) {
            return CompletableFuture.completedFuture(true);
        }
        if (startedTimestamp == 0L) {
            startedTimestamp = System.nanoTime();
        } else if (System.nanoTime() - startedTimestamp >= settings.disabledAfter().toNanos()) {
            registered = true;
            return CompletableFuture.completedFuture(true);
        }
        List<CompletableFuture<Boolean>> checks = new ArrayList<>();
        for (String name : settings.names()) {
            ActorRef region = shardRegionProvider.apply(name);
            CompletableFuture<Boolean> f = Patterns
                    .ask(region, ShardRegion.GetCurrentRegions.INSTANCE, settings.timeout())
                    .toCompletableFuture()
                    .thenApply(resp -> resp instanceof ShardRegion.CurrentRegions cr && !cr.regions().isEmpty())
                    .exceptionally(err -> false);
            checks.add(f);
        }
        return CompletableFuture.allOf(checks.toArray(CompletableFuture[]::new)).thenApply(v -> {
            boolean allOk = checks.stream().allMatch(CompletableFuture::join);
            if (allOk) {
                registered = true;
            }
            return allOk;
        });
    }
}
