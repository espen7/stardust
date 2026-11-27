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

import io.github.espen7.tapio.actor.AbstractActor;
import io.github.espen7.tapio.actor.Props;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.dispatch.RequiresMessageQueue;
import io.github.espen7.tapio.dispatch.UnboundedMessageQueueSemantics;

/**
 * INTERNAL API：接收集群心跳并立即回复 {@link ClusterMessages.HeartbeatRsp}。
 */
@InternalApi
public final class ClusterHeartbeatReceiver extends AbstractActor
        implements RequiresMessageQueue<UnboundedMessageQueueSemantics> {
    private final UniqueAddress selfUniqueAddress;

    /**
     * 构造心跳接收器。
     *
     * @param selfUniqueAddress 本节点唯一地址
     */
    public ClusterHeartbeatReceiver(UniqueAddress selfUniqueAddress) {
        this.selfUniqueAddress = selfUniqueAddress;
    }

    /** 创建 {@link Props}。 */
    public static Props props(UniqueAddress selfUniqueAddress) {
        return Props.create(ClusterHeartbeatReceiver.class, () -> new ClusterHeartbeatReceiver(selfUniqueAddress));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterMessages.Heartbeat.class, hb -> getSender().tell(
                        new ClusterMessages.HeartbeatRsp(selfUniqueAddress, hb.sequenceNr(), hb.creationTime()),
                        getSelf()))
                .build();
    }
}
