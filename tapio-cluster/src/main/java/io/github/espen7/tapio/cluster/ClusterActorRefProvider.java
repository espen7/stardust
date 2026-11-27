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
package io.github.espen7.tapio.cluster;

import io.github.espen7.tapio.actor.ActorRef;
import io.github.espen7.tapio.actor.ActorSystemImpl;
import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.remote.FailureDetectorRegistry;
import io.github.espen7.tapio.remote.RemoteActorRefProvider;

/**
 * 集群模式下的 {@link io.github.espen7.tapio.actor.ActorRefProvider} 实现。
 */
public class ClusterActorRefProvider extends RemoteActorRefProvider {
    /**
     * 构造 {@link ClusterActorRefProvider}（由 {@link ActorSystemImpl} 反射调用）。
     *
     * @param system 所属 {@link ActorSystemImpl}
     */
    public ClusterActorRefProvider(ActorSystemImpl system) {
        super(system);
    }

    @Override
    @InternalApi
    public void init(ActorSystemImpl sys) {
        super.init(sys);
        Cluster.INSTANCE.get(sys);
    }

    @Override
    protected ActorRef createRemoteWatcher(ActorSystemImpl sys, FailureDetectorRegistry<Address> fdRegistry) {
        return super.createRemoteWatcher(sys, fdRegistry);
    }
}
