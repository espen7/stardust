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
package io.github.espen7.tapio.cluster.sharding.external;

import io.github.espen7.tapio.Done;
import io.github.espen7.tapio.actor.Address;

import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * 外部分片分配客户端接口，用于动态更新期望的分片所在节点。
 */
public interface ExternalShardAllocationClient {

    /**
     * 更新单个分片的期望节点地址。
     */
    CompletionStage<Done> updateShardLocation(String shardId, Address location);

    /**
     * 批量更新多个分片的期望节点地址。
     */
    CompletionStage<Done> updateShardLocations(Map<String, Address> locations);
}
