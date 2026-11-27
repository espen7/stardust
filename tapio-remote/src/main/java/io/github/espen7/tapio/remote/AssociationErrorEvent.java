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
import io.github.espen7.tapio.event.Logging;

import java.io.Serial;
import java.util.Objects;

/**
 * 节点间关联发生错误事件。
 */
public final class AssociationErrorEvent extends AssociationEvent {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Throwable cause;
    private final Address localAddress;
    private final Address remoteAddress;
    private final boolean inbound;
    private final LogEvent.Level logLevel;

    /**
     * 构造关联错误事件。
     *
     * @param cause 异常原因
     * @param localAddress 本地地址
     * @param remoteAddress 远端地址
     * @param inbound 是否为入站连接
     * @param logLevel 建议日志级别
     */
    public AssociationErrorEvent(
            Throwable cause,
            Address localAddress,
            Address remoteAddress,
            boolean inbound,
            LogEvent.Level logLevel) {
        this.cause = Objects.requireNonNull(cause, "cause");
        this.localAddress = Objects.requireNonNull(localAddress, "localAddress");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
        this.inbound = inbound;
        this.logLevel = Objects.requireNonNull(logLevel, "logLevel");
    }

    /**
     * 返回导致关联失败的异常。
     *
     * @return 异常原因
     */
    public Throwable cause() {
        return cause;
    }

    /**
     * Java API：获取异常原因。
     *
     * @return 异常原因
     */
    public Throwable getCause() {
        return cause;
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
        return "AssociationError";
    }

    @Override
    public LogEvent.Level logLevel() {
        return logLevel;
    }

    @Override
    public String toString() {
        return super.toString() + ": Error [" + cause.getMessage() + "] [" + Logging.stackTraceFor(cause) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AssociationErrorEvent other
                && inbound == other.inbound
                && Objects.equals(cause, other.cause)
                && localAddress.equals(other.localAddress)
                && remoteAddress.equals(other.remoteAddress)
                && logLevel == other.logLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cause, localAddress, remoteAddress, inbound, logLevel);
    }
}
