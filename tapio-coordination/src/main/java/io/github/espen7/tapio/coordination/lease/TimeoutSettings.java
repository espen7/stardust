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
package io.github.espen7.tapio.coordination.lease;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;

import java.time.Duration;
import java.util.Objects;

/**
 * 租约心跳与操作超时配置。
 */
public final class TimeoutSettings {

    private final Duration heartbeatInterval;
    private final Duration heartbeatTimeout;
    private final Duration operationTimeout;

    public TimeoutSettings(Duration heartbeatInterval, Duration heartbeatTimeout, Duration operationTimeout) {
        this.heartbeatInterval = Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        this.heartbeatTimeout = Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
        if (heartbeatInterval.compareTo(heartbeatTimeout.dividedBy(2)) >= 0) {
            throw new IllegalArgumentException("heartbeat-interval must be less than half heartbeat-timeout");
        }
    }

    public static TimeoutSettings create(Config config) {
        Duration hbTimeout = config.getDuration("heartbeat-timeout");
        Duration hbInterval;
        if (config.getValue("heartbeat-interval").valueType() == ConfigValueType.STRING
                && config.getString("heartbeat-interval").isEmpty()) {
            Duration tenth = hbTimeout.dividedBy(10);
            hbInterval = tenth.compareTo(Duration.ofSeconds(5)) >= 0 ? tenth : Duration.ofSeconds(5);
        } else {
            hbInterval = config.getDuration("heartbeat-interval");
        }
        Duration opTimeout = config.getDuration("lease-operation-timeout");
        return new TimeoutSettings(hbInterval, hbTimeout, opTimeout);
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public Duration getOperationTimeout() {
        return operationTimeout;
    }

    public TimeoutSettings withHeartbeatInterval(Duration heartbeatInterval) {
        return new TimeoutSettings(heartbeatInterval, heartbeatTimeout, operationTimeout);
    }

    public TimeoutSettings withHeartbeatTimeout(Duration heartbeatTimeout) {
        return new TimeoutSettings(heartbeatInterval, heartbeatTimeout, operationTimeout);
    }

    public TimeoutSettings withOperationTimeout(Duration operationTimeout) {
        return new TimeoutSettings(heartbeatInterval, heartbeatTimeout, operationTimeout);
    }

    @Override
    public String toString() {
        return "TimeoutSettings(" + heartbeatInterval + ", " + heartbeatTimeout + ", " + operationTimeout + ")";
    }
}
