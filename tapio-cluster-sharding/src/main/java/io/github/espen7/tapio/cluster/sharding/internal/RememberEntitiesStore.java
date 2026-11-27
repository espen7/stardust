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

import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;

import java.util.Set;

/**
 * INTERNAL API：记住实体（Remember Entities）存储 SPI 协议与提供者契约。
 */
@InternalApi
public final class RememberEntitiesStore {
    private RememberEntitiesStore() {
    }

    /** 记住实体提供者。 */
    public interface Provider {
        Props coordinatorStoreProps();

        Props shardStoreProps(String shardId);
    }

    /** 分片级记住实体存储协议。 */
    public static final class ShardStore {
        private ShardStore() {
        }

        public record Update(Set<String> started, Set<String> stopped) {
        }

        public record UpdateDone(Set<String> started, Set<String> stopped) {
        }

        public enum GetEntities {
            INSTANCE
        }

        public record RememberedEntities(Set<String> entities) {
        }
    }

    /** 协调器级记住分片存储协议。 */
    public static final class CoordinatorStore {
        private CoordinatorStore() {
        }

        public record AddShard(String shardId) {
        }

        public record UpdateDone(String shardId) {
        }

        public record UpdateFailed(String shardId) {
        }

        public enum GetShards {
            INSTANCE
        }

        public record RememberedShards(Set<String> shards) {
        }
    }
}
