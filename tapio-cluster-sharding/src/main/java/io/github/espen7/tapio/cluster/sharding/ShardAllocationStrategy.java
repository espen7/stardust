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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * 分片分配与再平衡策略接口。
 */
public interface ShardAllocationStrategy {

    /**
     * 当协调器启动时通知策略关联的 {@link ActorSystem}。
     *
     * @param system ActorSystem
     */
    default void start(ActorSystem system) {
    }

    /**
     * 为未分配的分片选择目标 {@link ShardRegion}。
     *
     * @param requester 请求分配的 ShardRegion
     * @param shardId 分片标识
     * @param currentShardAllocations 当前各 region 已分配的分片列表
     * @return 目标 ShardRegion
     */
    CompletionStage<ActorRef> allocateShard(
            ActorRef requester,
            String shardId,
            Map<ActorRef, List<String>> currentShardAllocations);

    /**
     * 计算本轮需要再平衡迁移的分片集合。
     *
     * @param currentShardAllocations 当前各 region 已分配的分片列表
     * @param rebalanceInProgress 正在再平衡中的分片集合
     * @return 需要迁移的分片集合
     */
    CompletionStage<Set<String>> rebalance(
            Map<ActorRef, List<String>> currentShardAllocations,
            Set<String> rebalanceInProgress);
}
