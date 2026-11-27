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
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.cluster.sharding.ClusterShardingSettings;

/**
 * INTERNAL API：基于 DData 的记住实体存储提供者。
 */
@InternalApi
public final class DDataRememberEntitiesProvider implements RememberEntitiesStore.Provider {

    private final String typeName;
    private final ClusterShardingSettings settings;
    private final int majorityMinCap;
    private final ActorRef replicator;

    public DDataRememberEntitiesProvider(
            String typeName,
            ClusterShardingSettings settings,
            int majorityMinCap,
            ActorRef replicator) {
        this.typeName = typeName;
        this.settings = settings;
        this.majorityMinCap = majorityMinCap;
        this.replicator = replicator;
    }

    @Override
    public Props coordinatorStoreProps() {
        return DDataRememberEntitiesCoordinatorStore.props(typeName, settings, replicator, majorityMinCap);
    }

    @Override
    public Props shardStoreProps(String shardId) {
        return DDataRememberEntitiesShardStore.props(shardId, typeName, settings, replicator, majorityMinCap);
    }
}
