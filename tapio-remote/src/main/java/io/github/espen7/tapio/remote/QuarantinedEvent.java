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
package io.github.espen7.tapio.remote;

import io.github.espen7.tapio.actor.Address;
import io.github.espen7.tapio.event.LogEvent;

import java.io.Serial;
import java.util.Objects;

/**
 * 远端节点 UID 被隔离（Quarantined）事件。
 *
 * @param address 远端节点地址
 * @param longUid 远端节点实例 UID
 */
public record QuarantinedEvent(Address address, long longUid) implements RemotingLifecycleEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验地址非空。
     *
     * @param address 远端节点地址
     * @param longUid 远端节点实例 UID
     */
    public QuarantinedEvent {
        Objects.requireNonNull(address, "address");
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.WARNING;
    }

    @Override
    public String toString() {
        return "Association to [" + address + "] having UID [" + longUid
                + "] is irrecoverably failed. UID is now quarantined and all "
                + "messages to this UID will be delivered to dead letters. Remote ActorSystem must be restarted to recover "
                + "from this situation.";
    }
}
