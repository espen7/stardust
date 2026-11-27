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

import io.github.espen7.tapio.annotation.InternalApi;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * INTERNAL API：保存特定节点地址的心跳间隔滑动窗口统计信息。
 */
@InternalApi
final class HeartbeatHistory {
    private final int maxSampleSize;
    private final Deque<Long> intervals;
    private final long intervalSum;
    private final long squaredIntervalSum;

    private HeartbeatHistory(int maxSampleSize, Deque<Long> intervals, long intervalSum, long squaredIntervalSum) {
        if (maxSampleSize < 1) {
            throw new IllegalArgumentException("maxSampleSize must be >= 1, got [" + maxSampleSize + "]");
        }
        if (intervalSum < 0L) {
            throw new IllegalArgumentException("intervalSum must be >= 0, got [" + intervalSum + "]");
        }
        if (squaredIntervalSum < 0L) {
            throw new IllegalArgumentException("squaredIntervalSum must be >= 0, got [" + squaredIntervalSum + "]");
        }
        this.maxSampleSize = maxSampleSize;
        this.intervals = intervals;
        this.intervalSum = intervalSum;
        this.squaredIntervalSum = squaredIntervalSum;
    }

    static HeartbeatHistory empty(int maxSampleSize) {
        return new HeartbeatHistory(maxSampleSize, new ArrayDeque<>(), 0L, 0L);
    }

    double mean() {
        return (double) intervalSum / intervals.size();
    }

    double variance() {
        double m = mean();
        return ((double) squaredIntervalSum / intervals.size()) - (m * m);
    }

    double stdDeviation() {
        return Math.sqrt(variance());
    }

    HeartbeatHistory append(long interval) {
        ArrayDeque<Long> next = new ArrayDeque<>(intervals);
        long nextSum = intervalSum;
        long nextSqSum = squaredIntervalSum;
        if (next.size() >= maxSampleSize) {
            long oldest = next.removeFirst();
            nextSum -= oldest;
            nextSqSum -= oldest * oldest;
        }
        next.addLast(interval);
        nextSum += interval;
        nextSqSum += interval * interval;
        return new HeartbeatHistory(maxSampleSize, next, nextSum, nextSqSum);
    }
}
