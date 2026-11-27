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
 * 节点间关联断开事件。
 */
public final class DisassociatedEvent extends AssociationEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Address localAddress;
    private final Address remoteAddress;
    private final boolean inbound;

    /**
     * 构造关联断开事件。
     *
     * @param localAddress 本地地址
     * @param remoteAddress 远端地址
     * @param inbound 是否为入站连接
     */
    public DisassociatedEvent(Address localAddress, Address remoteAddress, boolean inbound) {
        this.localAddress = Objects.requireNonNull(localAddress, "localAddress");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
        this.inbound = inbound;
    }

    @Override
    public Address localAddress() {
        return localAddress;
    }

    @Override
    public Address remoteAddress() {
        return remoteAddress;
    }

    @Override
    public boolean inbound() {
        return inbound;
    }

    @Override
    protected String eventName() {
        return "Disassociated";
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.DEBUG;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DisassociatedEvent other
                && inbound == other.inbound
                && localAddress.equals(other.localAddress)
                && remoteAddress.equals(other.remoteAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(localAddress, remoteAddress, inbound);
    }
}
