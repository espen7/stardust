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
import io.github.espen7.tapio.annotation.InternalApi;
import io.github.espen7.tapio.event.EventStream;
import io.github.espen7.tapio.event.Logging;
import io.github.espen7.tapio.event.Warning;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Phi Accrual 失效检测器实现（Hayashibara 等人论文）。
 */
public class PhiAccrualFailureDetector implements FailureDetector, FailureDetectorWithAddress {
    private final double threshold;
    private final int maxSampleSize;
    private final Duration minStdDeviation;
    private final Duration acceptableHeartbeatPause;
    private final Duration firstHeartbeatEstimate;
    private final Optional<EventStream> eventStream;
    private final Clock clock;
    private final HeartbeatHistory firstHeartbeat;
    private final long acceptableHeartbeatPauseMillis;
    private final double minStdDeviationMillis;
    private volatile String address = "N/A";
    private final AtomicReference<State> state;

    private record State(HeartbeatHistory history, Long timestamp) {
    }

    /**
     * 构造 Phi Accrual 失效检测器。
     *
     * @param threshold 怀疑阈值 phi
     * @param maxSampleSize 历史样本上限
     * @param minStdDeviation 最小标准差
     * @param acceptableHeartbeatPause 可接受的心跳暂停时长
     * @param firstHeartbeatEstimate 首次心跳间隔估计值
     * @param eventStream 可选的事件总线
     * @param clock 单调时钟
     */
    public PhiAccrualFailureDetector(
            double threshold,
            int maxSampleSize,
            Duration minStdDeviation,
            Duration acceptableHeartbeatPause,
            Duration firstHeartbeatEstimate,
            Optional<EventStream> eventStream,
            Clock clock) {
        if (threshold <= 0.0) throw new IllegalArgumentException("failure-detector.threshold must be > 0");
        if (maxSampleSize <= 0) throw new IllegalArgumentException("failure-detector.max-sample-size must be > 0");
        if (minStdDeviation.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("failure-detector.min-std-deviation must be > 0");
        }
        if (acceptableHeartbeatPause.compareTo(Duration.ZERO) < 0) {
            throw new IllegalArgumentException("failure-detector.acceptable-heartbeat-pause must be >= 0");
        }
        if (firstHeartbeatEstimate.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("failure-detector.heartbeat-interval must be > 0");
        }
        this.threshold = threshold;
        this.maxSampleSize = maxSampleSize;
        this.minStdDeviation = minStdDeviation;
        this.acceptableHeartbeatPause = acceptableHeartbeatPause;
        this.firstHeartbeatEstimate = firstHeartbeatEstimate;
        this.eventStream = eventStream == null ? Optional.empty() : eventStream;
        this.clock = clock == null ? DEFAULT_CLOCK : clock;
        long mean = firstHeartbeatEstimate.toMillis();
        long stdDev = mean / 4;
        this.firstHeartbeat = HeartbeatHistory.empty(maxSampleSize).append(mean - stdDev).append(mean + stdDev);
        this.acceptableHeartbeatPauseMillis = acceptableHeartbeatPause.toMillis();
        this.minStdDeviationMillis = minStdDeviation.toMillis();
        this.state = new AtomicReference<>(new State(this.firstHeartbeat, null));
    }

    /**
     * 从配置构造 Phi Accrual 失效检测器（供 {@code DynamicAccess} 反射实例化使用）。
     *
     * @param config 失效检测器配置节
     * @param ev 事件总线
     */
    public PhiAccrualFailureDetector(Config config, EventStream ev) {
        this(
                config.getDouble("threshold"),
                config.getInt("max-sample-size"),
                config.getDuration("min-std-deviation"),
                config.getDuration("acceptable-heartbeat-pause"),
                config.getDuration("heartbeat-interval"),
                Optional.ofNullable(ev),
                DEFAULT_CLOCK);
    }

    @Override
    public void setAddress(String addr) {
        this.address = addr;
    }

    @Override
    public boolean isAvailable() {
        return isAvailable(clock.apply());
    }

    private boolean isAvailable(long timestamp) {
        return phi(timestamp) < threshold;
    }

    @Override
    public boolean isMonitoring() {
        return state.get().timestamp() != null;
    }

    @Override
    public final void heartbeat() {
        long timestamp = clock.apply();
        while (true) {
            State oldState = state.get();
            HeartbeatHistory newHistory;
            if (oldState.timestamp() == null) {
                newHistory = firstHeartbeat;
            } else {
                long interval = timestamp - oldState.timestamp();
                if (isAvailable(timestamp)) {
                    newHistory = recordInterval(interval, oldState.history());
                } else {
                    newHistory = oldState.history();
                }
            }
            State newState = new State(newHistory, timestamp);
            if (state.compareAndSet(oldState, newState)) {
                return;
            }
        }
    }

    /**
     * INTERNAL API：记录心跳间隔并在间隔过大时发布告警。
     *
     * @param interval 心跳间隔毫秒数
     * @param history 当前历史窗口
     * @return 更新后的历史窗口
     */
    @InternalApi
    protected HeartbeatHistory recordInterval(long interval, HeartbeatHistory history) {
        if (acceptableHeartbeatPauseMillis > 0 && interval >= (acceptableHeartbeatPauseMillis / 3 * 2)
                && eventStream.isPresent()) {
            eventStream.get().publish(new Warning(
                    toString(),
                    getClass(),
                    "heartbeat interval is growing too large for address " + address + ": " + interval + " millis",
                    (io.github.espen7.tapio.event.LogMarker) null));
        }
        return history.append(interval);
    }

    /**
     * 计算当前时刻的怀疑值 phi。
     *
     * @return phi 值
     */
    public double phi() {
        return phi(clock.apply());
    }

    private double phi(long timestamp) {
        State oldState = state.get();
        Long oldTimestamp = oldState.timestamp();
        if (oldTimestamp == null) {
            return 0.0;
        }
        long timeDiff = timestamp - oldTimestamp;
        HeartbeatHistory history = oldState.history();
        double mean = history.mean();
        double stdDeviation = Math.max(history.stdDeviation(), minStdDeviationMillis);
        return phi(timeDiff, mean + acceptableHeartbeatPauseMillis, stdDeviation);
    }

    static double phi(long timeDiff, double mean, double stdDeviation) {
        double y = (timeDiff - mean) / stdDeviation;
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        if (timeDiff > mean) {
            return -Math.log10(e / (1.0 + e));
        }
        return -Math.log10(1.0 - 1.0 / (1.0 + e));
    }

    /**
     * 返回配置的阈值。
     *
     * @return phi 阈值
     */
    public double threshold() {
        return threshold;
    }

    /**
     * 返回最大样本数。
     *
     * @return 最大样本数
     */
    public int maxSampleSize() {
        return maxSampleSize;
    }

    /**
     * 返回最小标准差。
     *
     * @return 最小标准差
     */
    public Duration minStdDeviation() {
        return minStdDeviation;
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
     * 返回首次心跳估计间隔。
     *
     * @return 首次心跳估计间隔
     */
    public Duration firstHeartbeatEstimate() {
        return firstHeartbeatEstimate;
    }
}
