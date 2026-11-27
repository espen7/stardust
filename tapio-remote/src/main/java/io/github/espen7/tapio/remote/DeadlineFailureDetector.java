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

import com.typesafe.config.Config;
import io.github.espen7.tapio.event.EventStream;

import java.time.Duration;

/**
 * 基于固定截止超时判定可用性的失效检测器。
 */
public class DeadlineFailureDetector implements FailureDetector {
    private final Duration acceptableHeartbeatPause;
    private final Duration heartbeatInterval;
    private final Clock clock;
    private final long deadlineMillis;
    private volatile long heartbeatTimestamp = 0L;
    private volatile boolean active = false;

    /**
     * 构造固定超时失效检测器。
     *
     * @param acceptableHeartbeatPause 可接受的心跳暂停时长
     * @param heartbeatInterval 期望的心跳间隔
     * @param clock 单调时钟
     */
    public DeadlineFailureDetector(Duration acceptableHeartbeatPause, Duration heartbeatInterval, Clock clock) {
        if (acceptableHeartbeatPause.compareTo(Duration.ZERO) < 0) {
            throw new IllegalArgumentException("failure-detector.acceptable-heartbeat-pause must be >= 0 s");
        }
        if (heartbeatInterval.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("failure-detector.heartbeat-interval must be > 0 s");
        }
        this.acceptableHeartbeatPause = acceptableHeartbeatPause;
        this.heartbeatInterval = heartbeatInterval;
        this.clock = clock == null ? DEFAULT_CLOCK : clock;
        this.deadlineMillis = acceptableHeartbeatPause.toMillis() + heartbeatInterval.toMillis();
    }

    /**
     * 从配置构造固定超时失效检测器（供 {@code DynamicAccess} 反射实例化使用）。
     *
     * @param config 失效检测器配置节
     * @param ev 事件总线
     */
    public DeadlineFailureDetector(Config config, EventStream ev) {
        this(config.getDuration("acceptable-heartbeat-pause"), config.getDuration("heartbeat-interval"), DEFAULT_CLOCK);
    }

    @Override
    public boolean isAvailable() {
        return !active || (heartbeatTimestamp + deadlineMillis) > clock.apply();
    }

    @Override
    public boolean isMonitoring() {
        return active;
    }

    @Override
    public final void heartbeat() {
        heartbeatTimestamp = clock.apply();
        active = true;
    }

    /**
     * 返回可接受的心跳暂停时长。
     *
     * @return 可接受暂停时长
     */
    public Duration acceptableHeartbeatPause() {
        return acceptableHeartbeatPause;
    }

    /**
     * 返回期望的心跳间隔。
     *
     * @return 心跳间隔
     */
    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }
}
