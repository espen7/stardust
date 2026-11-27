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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 远程传输开始监听地址事件。
 *
 * @param listenAddresses 已绑定的监听地址集合
 */
public record RemotingListenEvent(Set<Address> listenAddresses) implements RemotingLifecycleEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 防御性复制监听地址集合。
     *
     * @param listenAddresses 监听地址集合
     */
    public RemotingListenEvent {
        listenAddresses = Set.copyOf(Objects.requireNonNull(listenAddresses, "listenAddresses"));
    }

    /**
     * Java API：获取监听地址集合。
     *
     * @return 不可变地址集合
     */
    public Set<Address> getListenAddresses() {
        return listenAddresses;
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.INFO;
    }

    @Override
    public String toString() {
        return "Remoting now listens on addresses: "
                + listenAddresses.stream().map(Address::toString).collect(Collectors.joining(", ", "[", "]"));
    }
}
