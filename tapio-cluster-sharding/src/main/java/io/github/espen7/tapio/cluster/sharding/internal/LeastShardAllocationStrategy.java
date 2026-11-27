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
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * INTERNAL API：最少分片分配与再平衡策略。
 */
@InternalApi
public class LeastShardAllocationStrategy extends AbstractLeastShardAllocationStrategy {

    private static final CompletionStage<Set<String>> EMPTY_REBALANCE_RESULT =
            CompletableFuture.completedFuture(Set.of());

    private final int absoluteLimit;
    private final double relativeLimit;
    private final int rebalanceThreshold;
    private final int maxSimultaneousRebalance;

    /**
     * 使用绝对/相对上限构造策略。
     */
    public LeastShardAllocationStrategy(int absoluteLimit, double relativeLimit) {
        this(absoluteLimit, relativeLimit, 1, 3);
    }

    /**
     * 使用显式阈值构造策略。
     */
    public LeastShardAllocationStrategy(
            int absoluteLimit,
            double relativeLimit,
            int rebalanceThreshold,
            int maxSimultaneousRebalance) {
        this.absoluteLimit = absoluteLimit;
        this.relativeLimit = relativeLimit;
        this.rebalanceThreshold = Math.max(1, rebalanceThreshold);
        this.maxSimultaneousRebalance = Math.max(1, maxSimultaneousRebalance);
    }

    @Override
    public CompletionStage<Set<String>> rebalance(
            Map<ActorRef, List<String>> currentShardAllocations,
            Set<String> rebalanceInProgress) {
        if (!rebalanceInProgress.isEmpty() || currentShardAllocations.size() <= 1) {
            return EMPTY_REBALANCE_RESULT;
        }
        List<RegionEntry> regionEntries = regionEntriesFor(currentShardAllocations);
        if (regionEntries.size() <= 1 || !isAGoodTimeToRebalance(regionEntries)) {
            return EMPTY_REBALANCE_RESULT;
        }
        List<RegionEntry> sorted = new ArrayList<>(regionEntries);
        sorted.sort(SHARD_SUITABILITY_ORDERING);
        int minShards = sorted.getFirst().shardIds().size();
        int maxShards = sorted.getLast().shardIds().size();

        int totalShards = sorted.stream().mapToInt(e -> e.shardIds().size()).sum();
        int limit;
        if (absoluteLimit > 0) {
            int rel = Math.max(1, (int) Math.round(totalShards * relativeLimit));
            limit = Math.min(absoluteLimit, rel);
        } else {
            limit = maxSimultaneousRebalance;
        }
        int threshold = absoluteLimit > 0 ? 1 : rebalanceThreshold;
        if (maxShards - minShards <= threshold) {
            return EMPTY_REBALANCE_RESULT;
        }

        int diff = maxShards - minShards;
        int numberOfShards = Math.min(Math.max(1, diff / 2), limit);
        List<String> candidateShards = sorted.getLast().shardIds();
        Set<String> selected = new HashSet<>();
        for (int i = 0; i < Math.min(numberOfShards, candidateShards.size()); i++) {
            selected.add(candidateShards.get(i));
        }
        return CompletableFuture.completedFuture(selected);
    }
}
