/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.espen7.tapio.pattern;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 指数退避延迟计算工具，对应 Pekko {@code RetrySupport}。
 */
public final class RetrySupport {
    private RetrySupport() {
    }

    /**
     * 计算第 {@code restartCount} 次重启的指数退避延迟（受 {@code maxBackoff} 上限约束并附加随机抖动）。
     *
     * @param restartCount 当前重启次数
     * @param minBackoff 最小退避时长
     * @param maxBackoff 最大退避时长
     * @param randomFactor 随机抖动因子
     * @return 计算得到的退避时长
     */
    public static Duration calculateDelay(int restartCount, Duration minBackoff, Duration maxBackoff, double randomFactor) {
        double rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor;
        double baseNanos = minBackoff.toNanos() * Math.pow(2.0, restartCount);
        double maxNanos = maxBackoff.toNanos();
        double cappedNanos = Math.min(maxNanos, baseNanos) * rnd;
        if (!Double.isFinite(cappedNanos) || cappedNanos >= Long.MAX_VALUE) {
            return maxBackoff;
        }
        return Duration.ofNanos((long) cappedNanos);
    }
}
