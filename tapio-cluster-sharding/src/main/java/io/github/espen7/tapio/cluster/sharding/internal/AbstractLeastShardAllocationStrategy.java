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
package io.github.espen7.tapio.cluster.sharding.internal;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystem;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.ClusterEvent;
import io.github.espen7.tapio.cluster.Member;
import io.github.espen7.tapio.cluster.sharding.ShardAllocationStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * INTERNAL API：最少分片分配策略抽象基类。
 */
@InternalApi
public abstract class AbstractLeastShardAllocationStrategy
        implements ShardAllocationStrategy, ClusterShardAllocationMixin {

    private volatile ActorSystem system;
    private volatile Cluster cluster;

    @Override
    public void start(ActorSystem system) {
        this.system = system;
        this.cluster = Cluster.get(system);
    }

    @Override
    public ClusterEvent.CurrentClusterState clusterState() {
        return cluster.state();
    }

    @Override
    public Member selfMember() {
        return cluster.selfMember();
    }

    @Override
    public CompletionStage<ActorRef> allocateShard(
            ActorRef requester,
            String shardId,
            Map<ActorRef, List<String>> currentShardAllocations) {
        List<RegionEntry> regionEntries = regionEntriesFor(currentShardAllocations);
        if (regionEntries.isEmpty()) {
            return CompletableFuture.completedFuture(requester);
        }
        RegionEntry chosen = mostSuitableRegion(regionEntries);
        return CompletableFuture.completedFuture(chosen.region());
    }

    /**
     * 按 {@link ClusterShardAllocationMixin#SHARD_SUITABILITY_ORDERING} 选出最适合承载新分片的 Region。
     */
    protected final RegionEntry mostSuitableRegion(List<RegionEntry> regionEntries) {
        List<RegionEntry> sorted = new ArrayList<>(regionEntries);
        sorted.sort(SHARD_SUITABILITY_ORDERING);
        return sorted.getFirst();
    }
}
