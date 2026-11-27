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
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.cluster.Cluster;
import io.github.espen7.tapio.cluster.sharding.internal.AbstractLeastShardAllocationStrategy;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.LoggingAdapter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 基于一致性哈希环的分片分配与再平衡策略，适合多实体类型同分片 ID 尽量同节点部署的场景。
 */
public class ConsistentHashingShardAllocationStrategy extends AbstractLeastShardAllocationStrategy {

    private static final int VIRTUAL_NODES_FACTOR = 10;
    private static final CompletionStage<Set<String>> EMPTY_REBALANCE_RESULT =
            CompletableFuture.completedFuture(Set.of());

    private final int rebalanceLimit;
    private volatile LoggingAdapter log;
    private List<Address> hashedByNodes = List.of();
    private final NavigableMap<Integer, Address> ring = new TreeMap<>();

    public ConsistentHashingShardAllocationStrategy(int rebalanceLimit) {
        this.rebalanceLimit = rebalanceLimit;
    }

    @Override
    public void start(ActorSystem system) {
        super.start(system);
        this.log = Logging.getLogger(system, this, getClass());
    }

    @Override
    public CompletionStage<ActorRef> allocateShard(
            ActorRef requester,
            String shardId,
            Map<ActorRef, List<String>> currentShardAllocations) {
        if (currentShardAllocations.isEmpty()) {
            return CompletableFuture.completedFuture(requester);
        }
        List<Address> nodes = nodesForRegions(currentShardAllocations);
        updateHashing(nodes);
        Address targetNode = nodeFor(shardId);
        for (ActorRef region : currentShardAllocations.keySet()) {
            if (nodeForRegion(region).equals(targetNode)) {
                return CompletableFuture.completedFuture(region);
            }
        }
        throw new IllegalStateException("currentShardAllocations should include region for node [" + targetNode + "]");
    }

    @Override
    public CompletionStage<Set<String>> rebalance(
            Map<ActorRef, List<String>> currentShardAllocations,
            Set<String> rebalanceInProgress) {
        List<RegionEntry> regionEntries = regionEntriesFor(currentShardAllocations);
        if (!isAGoodTimeToRebalance(regionEntries)) {
            return EMPTY_REBALANCE_RESULT;
        }
        List<Address> nodes = nodesForRegions(currentShardAllocations);
        updateHashing(nodes);

        Map<Address, ActorRef> regionByNode = new HashMap<>();
        for (ActorRef region : currentShardAllocations.keySet()) {
            regionByNode.put(nodeForRegion(region), region);
        }

        Set<String> result = new HashSet<>();
        List<Map.Entry<ActorRef, List<String>>> sortedEntries = new ArrayList<>(currentShardAllocations.entrySet());
        sortedEntries.sort(Comparator.comparing(e -> nodeForRegion(e.getKey()), Address.ADDRESS_ORDERING));

        for (Map.Entry<ActorRef, List<String>> entry : sortedEntries) {
            ActorRef currentRegion = entry.getKey();
            for (String shardId : entry.getValue()) {
                if ((rebalanceLimit <= 0 || result.size() < rebalanceLimit)
                        && !rebalanceInProgress.contains(shardId)) {
                    Address targetNode = nodeFor(shardId);
                    ActorRef targetRegion = regionByNode.get(targetNode);
                    if (targetRegion != null && !targetRegion.equals(currentRegion)) {
                        if (log != null && log.isDebugEnabled()) {
                            log.debug("Rebalance needed for shard [{}], from [{}] to [{}]",
                                    shardId, nodeForRegion(currentRegion), targetNode);
                        }
                        result.add(shardId);
                    }
                }
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    private List<Address> nodesForRegions(Map<ActorRef, List<String>> currentShardAllocations) {
        List<Address> nodes = new ArrayList<>(currentShardAllocations.size());
        for (ActorRef region : currentShardAllocations.keySet()) {
            nodes.add(nodeForRegion(region));
        }
        return nodes;
    }

    private Address nodeForRegion(ActorRef region) {
        Address addr = region.path().address();
        return addr.host() == null ? selfMember().address() : addr;
    }

    private void updateHashing(List<Address> nodes) {
        List<Address> sortedNodes = new ArrayList<>(nodes);
        sortedNodes.sort(Address.ADDRESS_ORDERING);
        if (!sortedNodes.equals(hashedByNodes)) {
            hashedByNodes = List.copyOf(sortedNodes);
            ring.clear();
            for (Address node : sortedNodes) {
                for (int i = 0; i < VIRTUAL_NODES_FACTOR; i++) {
                    ring.put(fnv1aHash(node + "#" + i), node);
                }
            }
        }
    }

    private Address nodeFor(String shardId) {
        if (ring.isEmpty()) {
            return selfMember().address();
        }
        int hash = fnv1aHash(shardId);
        Map.Entry<Integer, Address> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    private static int fnv1aHash(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        int hash = 0x811c9dc5;
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 0x01000193;
        }
        return hash;
    }
}
