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

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.cluster.sharding.ClusterShardingSerializable;

import java.util.Map;
import java.util.Objects;

/**
 * 外部显式指定的分片位置映射。
 */
public record ShardLocations(Map<String, Address> locations) implements ClusterShardingSerializable {
    public static final ShardLocations EMPTY = new ShardLocations(Map.of());

    public ShardLocations {
        locations = locations == null ? Map.of() : Map.copyOf(locations);
    }

    public ShardLocations withUpdated(Map<String, Address> updates) {
        Objects.requireNonNull(updates, "updates");
        java.util.HashMap<String, Address> next = new java.util.HashMap<>(locations);
        next.putAll(updates);
        return new ShardLocations(next);
    }
}
