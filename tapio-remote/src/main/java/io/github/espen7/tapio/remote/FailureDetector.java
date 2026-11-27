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

import java.util.concurrent.TimeUnit;

/**
 * 线程安全的失效检测器接口，用于记录心跳并判断被监控资源是否可用。
 */
public interface FailureDetector {

    /**
     * 返回被监控资源当前是否被判定为可用且健康。
     *
     * @return 可用时返回 {@code true}
     */
    boolean isAvailable();

    /**
     * 返回是否已收到至少一次心跳并开始监控。
     *
     * @return 已开始监控时返回 {@code true}
     */
    boolean isMonitoring();

    /**
     * 通知失效检测器收到来自被监控资源的心跳。
     */
    void heartbeat();

    /**
     * 返回毫秒级单调时间的时钟抽象，仅用于测量时间间隔。
     */
    @FunctionalInterface
    interface Clock {
        /**
         * 返回当前毫秒时间戳。
         *
         * @return 毫秒时间
         */
        long apply();
    }

    /** 基于 {@link System#nanoTime()} 的默认单调时钟。 */
    Clock DEFAULT_CLOCK = () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
}
