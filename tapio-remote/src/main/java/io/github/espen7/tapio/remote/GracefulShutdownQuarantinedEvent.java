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

import io.github.espen7.tapio.event.LogEvent;

import java.io.Serial;
import java.util.Objects;

/**
 * 因正常关闭或集群离开导致的优雅隔离事件。
 *
 * @param uniqueAddress 被隔离的节点唯一地址
 * @param reason 隔离原因描述
 */
public record GracefulShutdownQuarantinedEvent(UniqueAddress uniqueAddress, String reason)
        implements RemotingLifecycleEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 校验字段非空。
     *
     * @param uniqueAddress 节点唯一地址
     * @param reason 原因描述
     */
    public GracefulShutdownQuarantinedEvent {
        Objects.requireNonNull(uniqueAddress, "uniqueAddress");
        Objects.requireNonNull(reason, "reason");
    }

    @Override
    public LogEvent.Level logLevel() {
        return LogEvent.Level.INFO;
    }

    @Override
    public String toString() {
        return "Association to [" + uniqueAddress.address() + "] having UID [" + uniqueAddress.uid()
                + "] has been stopped. All messages to this UID will be delivered to dead letters. Reason: " + reason + " ";
    }
}
