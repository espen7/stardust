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

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.actor.DeadLetterSuppression;
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.remote.HeartbeatMessage;

import java.util.Set;

/**
 * INTERNAL API：集群内部协议与管理命令消息定义。
 */
@InternalApi
public final class ClusterMessages {
    private ClusterMessages() {
    }

    /** 请求加入集群消息。 */
    public record Join(UniqueAddress node, Set<String> roles, String appVersion) implements ClusterMessage {
    }

    /** 种子节点欢迎响应，携带当前 Gossip 快照。 */
    public record Welcome(UniqueAddress from, Gossip gossip) implements ClusterMessage {
    }

    /** 种子节点初始化探测请求。 */
    public static final class InitJoin implements ClusterMessage {
        public static final InitJoin INSTANCE = new InitJoin();

        private InitJoin() {
        }
    }

    /** 种子节点初始化探测确认响应。 */
    public record InitJoinAck(Address address) implements ClusterMessage {
    }

    /** 种子节点初始化探测拒绝响应。 */
    public record InitJoinNack(Address address) implements ClusterMessage {
    }

    /** Gossip 状态摘要（用于对比向量时钟）。 */
    public record GossipStatus(UniqueAddress from, VectorClock version, Set<UniqueAddress> seen)
            implements ClusterMessage, DeadLetterSuppression {
    }

    /** Gossip 完整状态信封。 */
    public record GossipEnvelope(UniqueAddress from, UniqueAddress to, Gossip gossip)
            implements ClusterMessage, DeadLetterSuppression {
    }

    /** 集群节点间心跳请求。 */
    public record Heartbeat(Address from, long sequenceNr, long creationTime)
            implements ClusterMessage, HeartbeatMessage, DeadLetterSuppression {
    }

    /** 集群节点间心跳响应。 */
    public record HeartbeatRsp(UniqueAddress from, long sequenceNr, long creationTime)
            implements ClusterMessage, HeartbeatMessage, DeadLetterSuppression {
    }

    /** 用户请求节点优雅离开。 */
    public record Leave(Address address) implements ClusterMessage {
    }

    /** 用户或 SBR 请求将节点标记为 Down。 */
    public record Down(Address address) implements ClusterMessage {
    }

    /** 通知节点已被确认 Exiting 并准备关闭。 */
    public record ExitingConfirmed(UniqueAddress node) implements ClusterMessage {
    }

    /** 内部命令：加入指定地址。 */
    public record JoinTo(Address address) {
    }

    /** 内部命令：加入种子节点列表。 */
    public record JoinSeedNodes(java.util.List<Address> seedNodes) {
    }

    /** 内部命令：关闭集群。 */
    public static final class Shutdown {
        public static final Shutdown INSTANCE = new Shutdown();

        private Shutdown() {
        }
    }
}
